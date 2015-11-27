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
package net.oneandone.reactive.kafka.rest;


import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import net.oneandone.reactive.ConnectException;
import net.oneandone.reactive.ReactiveSource;
import net.oneandone.reactive.utils.Utils;
import net.oneandone.reactive.utils.SubscriberNotifier;



import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;




public class KafkaSource<K, V> implements Publisher<KafkaSource.TopicMessage<K, V>> {
    
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
    public ReactiveSource<KafkaSource.TopicMessage<K, V>> open() throws ConnectException {
        return Utils.get(openAsync());
    } 

    
    /**
     * @return the new source instance future
     */
    public CompletableFuture<ReactiveSource<KafkaSource.TopicMessage<K, V>>> openAsync() {
        return ReactiveSource.subscribeAsync(this);
    }
    
    
    
    @Override
    public void subscribe(Subscriber<? super KafkaSource.TopicMessage<K, V>> subscriber) {
        // https://github.com/reactive-streams/reactive-streams-jvm#1.9
        if (subscriber == null) {  
            throw new NullPointerException("subscriber is null");
        }
        
        new ConsumerSubscription<K, V>(topic, properties, subscriber);
    }
    
    
    
    private static class ConsumerSubscription<K, V> implements Subscription {
        
        private final SubscriberNotifier<KafkaSource.TopicMessage<K, V>> subscriberNotifier; 
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        
        private final InboundBuffer inboundBuffer;

        
        private ConsumerSubscription(String topic, 
                                     ImmutableMap<String, Object> properties, 
                                     Subscriber<? super KafkaSource.TopicMessage<K, V>> subscriber) {

            this.subscriberNotifier = new SubscriberNotifier<>(subscriber, this);

            
            Map<String, Object> props = Maps.newHashMap(properties); 
            props.put("enable.auto.commit", "false");
            
            KafkaConsumer<K, V> consumer = new KafkaConsumer<>(ImmutableMap.copyOf(props));
            
            ImmutableList<TopicPartition> partitions = ImmutableList.copyOf(consumer.partitionsFor(topic)
                                                                                    .stream()
                                                                                    .map(partition -> new TopicPartition(topic, partition.partition()))
                                                                                    .collect(Collectors.toList()));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions.toArray(new TopicPartition[partitions.size()]));
            
            this.inboundBuffer = new InboundBuffer(consumer, (record) -> subscriberNotifier.notifyOnNext(record));
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
            private final Queue<KafkaSource.TopicMessage<K, V>> bufferedRecords = Queues.newConcurrentLinkedQueue();
            private final Consumer<KafkaSource.TopicMessage<K, V>> recordConsumer;
            
            private final KafkaConsumer<K, V> consumer;

            private final AtomicInteger numPendingRequested = new AtomicInteger(0);

            
            public InboundBuffer(KafkaConsumer<K, V> consumer, Consumer<KafkaSource.TopicMessage<K, V>> recordConsumer) {
                this.consumer = consumer;
                this.recordConsumer = recordConsumer;
            }

            public void onRequested(int num) {
                numPendingRequested.addAndGet(num);
                process();
            }
            

            public ImmutableMap<Integer, Long> onRecords(ConsumerRecords<K, V> records) {
                Map<Integer, Long> consumedOffsets = Maps.newHashMap();
                
                for (ConsumerRecord<K, V> record : records) {
                    consumedOffsets.put(record.partition(), record.offset());
                    bufferedRecords.add(new TopicMessage<>(ImmutableMap.copyOf(consumedOffsets), record));
                }
                
                process();
                
                return ImmutableMap.copyOf(consumedOffsets); 
            }
            
            private void process() {
                while (numPendingRequested.get() > 0) {
                    KafkaSource.TopicMessage<K, V> record = bufferedRecords.poll();
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
                    ConsumerRecords<K, V> records = consumer.poll(100);
                    if (!records.isEmpty()) {
                        onRecords(records);
                    }
                }
            }
        }
    }
    
    
    
    
    public static class TopicMessage<K, V> {
        
        private final ImmutableMap<Integer, Long> consumedOffsets;
        private final ConsumerRecord<K, V> record;
        
        public TopicMessage(ImmutableMap<Integer, Long> consumedOffsets, ConsumerRecord<K, V> record) {
            this.consumedOffsets = consumedOffsets;
            this.record = record;
        }

        public ImmutableMap<Integer, Long> getConsumedOffsets() {
            return consumedOffsets;
        }

        public ConsumerRecord<K, V> getRecord() {
            return record;
        }
    }
}  