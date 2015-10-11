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


import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import net.oneandone.reactive.ConnectException;
import net.oneandone.reactive.ReactiveSource;
import net.oneandone.reactive.utils.Utils;
import net.oneandone.reactive.utils.SubscriberNotifier;


import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;



public class KafkaSource<K, V> implements Publisher<ConsumerRecord<K, V>> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
    
    // properties
    private final ImmutableMap<String, Object> properties;
    private final String topic; 
    

    public KafkaSource(String topic, ImmutableMap<String, Object> properties) {
        this.topic = topic;
        this.properties = properties;
    }

    
    
    public KafkaSource(String topic) {
        this(topic, ImmutableMap.of());
    }

    
    

    
    public KafkaSource<K, V> withProperty(String name, Object value) {
        return new KafkaSource<>(this.topic,
                                 ImmutableMap.<String, Object>builder().putAll(properties).put(name, value).build());
    }
    
    
    
    /**
     * @return the new source instance
     * @throws ConnectException if an connect error occurs
     */
    public ReactiveSource<ConsumerRecord<K, V>> open() throws ConnectException {
        return Utils.get(openAsync());
    } 

    
    /**
     * @return the new source instance future
     */
    public CompletableFuture<ReactiveSource<ConsumerRecord<K, V>>> openAsync() {
        return ReactiveSource.subscribeAsync(this);
    }
    
    
    
    @Override
    public void subscribe(Subscriber<? super ConsumerRecord<K, V>> subscriber) {
        // https://github.com/reactive-streams/reactive-streams-jvm#1.9
        if (subscriber == null) {  
            throw new NullPointerException("subscriber is null");
        }
        
        new ConsumerSubscription<K, V>(topic, properties, subscriber);
    }
    
    
    
    private static class ConsumerSubscription<K, V> implements Subscription {
        
        private final String topic; 
        private final SubscriberNotifier<ConsumerRecord<K, V>> subscriberNotifier; 
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        
        private final InboundBuffer inboundBuffer;

        
        private ConsumerSubscription(String topic, 
                                     ImmutableMap<String, Object> properties, 
                                     Subscriber<? super ConsumerRecord<K, V>> subscriber) {

            this.topic = topic;
            this.subscriberNotifier = new SubscriberNotifier<>(subscriber, this);

            Properties props = new Properties();
            props.putAll(properties);
     
            ConsumerConfig conf = new ConsumerConfig(props);
            ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(conf);
            
            Map<String, Integer> topicCountMap = Maps.newHashMap();
            topicCountMap.put(topic, 1);
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

            this.inboundBuffer = new InboundBuffer(streams.get(0), (record) -> subscriberNotifier.notifyOnNext(record));
            Thread t = new Thread(inboundBuffer);
            t.setDaemon(true);
            t.start(); 
            
            subscriberNotifier.start();
        }
 
     
        
        @Override
        public void cancel() {
            subscriberNotifier.notifyOnComplete();
        } 
        
        
        @Override
        public void request(long n) {
            if (isOpen.get()) {
                if(n < 0) {
                    // https://github.com/reactive-streams/reactive-streams#3.9
                    subscriberNotifier.notifyOnError((new IllegalArgumentException("Non-negative number of elements must be requested: https://github.com/reactive-streams/reactive-streams#3.9")));
                    
                } else {
                    inboundBuffer.onRequested((int) n);
                }
            } else {
                subscriberNotifier.notifyOnError((new IllegalArgumentException("source is closed")));
            }
        }


         
        @Override
        public String toString() {
           return "";
        }
        
        
        

        private class InboundBuffer implements Runnable {
            private final Queue<ConsumerRecord<K, V>> bufferedRecords = Queues.newConcurrentLinkedQueue();
            private final Consumer<ConsumerRecord<K, V>> recordConsumer;
            
            private final KafkaStream<byte[], byte[]> stream;
            private final ConsumerIterator<byte[], byte[]> it;

            private final AtomicInteger numPendingRequested = new AtomicInteger(0);

            
            public InboundBuffer(KafkaStream<byte[], byte[]> stream, Consumer<ConsumerRecord<K, V>> recordConsumer) {
                this.stream = stream;
                this.it = stream.iterator();
                this.recordConsumer = recordConsumer;
            }

            public void onRequested(int num) {
                numPendingRequested.addAndGet(num);
                process();
            }
            

            public void onRecord(ConsumerRecord<K, V> record) {
                bufferedRecords.add(record);
                process();
            }
            
            private void process() {
                while (numPendingRequested.get() > 0) {
                    ConsumerRecord<K, V> record = bufferedRecords.poll();
                    if (record == null) {
                        return;
                    } else {
                        recordConsumer.accept(record);
                        numPendingRequested.decrementAndGet();
                    }
                }
            }
            
            
            @Override
            public void run() {
                while (true) {
                    MessageAndMetadata<byte[], byte[]> mam = it.next();
                    onRecord(new ConsumerRecord<>(topic, 0, null, null, 0l));
                }
            }
        }
    }
}  