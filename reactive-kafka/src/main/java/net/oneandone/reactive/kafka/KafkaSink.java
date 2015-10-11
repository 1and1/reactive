/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.reactive.kafka;



import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import net.oneandone.reactive.ReactiveSink;
import net.oneandone.reactive.ConnectException;
import net.oneandone.reactive.utils.Utils;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;




public class KafkaSink<K, V> implements Subscriber<ProducerRecord<K, V>> {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);
    private static final int DEFAULT_BUFFER_SIZE = 50;
    
    // properties
    private final ImmutableMap<String, Object> properties;
    private final int numBufferedElements;
    
    // producer
    private final AtomicReference<Producer<K, V>> producerRef = new AtomicReference<>();
   
    

    private KafkaSink(ImmutableMap<String, Object> properties, int numBufferedElements) {
        this.properties = properties;
        this.numBufferedElements = numBufferedElements;
    }
    
    
    public KafkaSink() {
        this(ImmutableMap.of());
    }

    public KafkaSink(ImmutableMap<String, Object> properties) {
        this(properties, DEFAULT_BUFFER_SIZE);
    }
    
    public KafkaSink<K, V> buffer(int numBufferedElements) {
        return new KafkaSink<>(this.properties, 
                               numBufferedElements);
    }

    
    public KafkaSink<K, V> withProperty(String name, Object value) {
        return new KafkaSink<>(ImmutableMap.<String, Object>builder().putAll(properties).put(name, value).build(),
                               this.numBufferedElements);
    }
    
    
    
    public ReactiveSink<ProducerRecord<K, V>> open() {
        return Utils.get(openAsync());
    }
    
    
    /**
     * @return the new source instance future
     */
    public CompletableFuture<ReactiveSink<ProducerRecord<K, V>>> openAsync() {
        return ReactiveSink.buffersize(numBufferedElements).publishAsync(this)
                           .exceptionally(error -> { throw (error instanceof ConnectException) ? (ConnectException) error : new ConnectException(error); });
    }
    

    @Override
    public void onSubscribe(Subscription subscription) {
        if (producerRef.get() == null) {
            producerRef.set(new Producer<K, V>(properties, subscription));
        } else {
            throw new IllegalStateException("already subscribed");
        }
    }

    
    @Override
    public void onNext(ProducerRecord<K, V> record) {
        producerRef.get().write(record);
    }
    
    @Override
    public void onError(Throwable t) {
        producerRef.get().terminate(t);
    }
    
    @Override
    public void onComplete() {
        producerRef.get().close();
    }
    
    
    @Override
    public String toString() {
        Producer<K, V> producer = producerRef.get();
        return (producer == null) ? "<null>": producer.toString();
    }
     
    
    
    private static final class Producer<K, V> {
        private final KafkaProducer<K, V> kafkaProducer;
        private final Subscription subscription;
        
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        
        
        public Producer(ImmutableMap<String, Object> properties, Subscription subscription) {
            this.kafkaProducer = new KafkaProducer<K, V>(properties);
            this.subscription = subscription;
            subscription.request(1);
        }
        
        
        public void write(ProducerRecord<K, V> record) {
            
            Callback callback = new Callback() {
                
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        LOG.debug("submit into " + metadata.topic() + "  (" + metadata.offset() + "@" + metadata.partition() + ")");
                        subscription.request(1);
                    } else {
                        LOG.debug(exception.getMessage() + " error occured by writing data");
                        subscription.cancel();
                    }
                }
            };
            
            kafkaProducer.send(record, callback);
        }
        

        
        public Void terminate(Throwable t) {
            close();
            return null;
        }
        
     
        public void close() {
            if (isOpen.getAndSet(false)) {
                kafkaProducer.close();
                subscription.cancel();
            }
        }
        
        @Override
        public String toString() {
           return  kafkaProducer.toString();
        }
    }    
}