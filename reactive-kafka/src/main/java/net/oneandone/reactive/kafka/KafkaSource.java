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

import java.util.Optional;
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
import com.google.common.collect.Queues;



public class KafkaSource<K, V> {
    
    private final ImmutableMap<String, Object> properties;
    
    
    public KafkaSource(ImmutableMap<String, Object> properties) {
        this.properties = properties;
    }
 
    
    public SingleKafkaSource<K, V> withTopic(String topic) {
        return new SingleKafkaSourceImpl<K, V>(this.properties, topic, new KafkaMessageIdList(), new KafkaMessageIdList());
    }
    
    

    
    

    public static interface SingleKafkaSource<K, V> extends Publisher<KafkaMessage<K, V>> {
        
        
        /**
         * @return the new source instance
         * @throws ConnectException if an connect error occurs
         */
        default ReactiveSource<KafkaMessage<K, V>> open() throws ConnectException {
            return Utils.get(openAsync());
        } 

        
        /**
         * @return the new source instance future
         */
        default CompletableFuture<ReactiveSource<KafkaMessage<K, V>>> openAsync() {
            return ReactiveSource.subscribeAsync(this);
        }
        
        
        SingleKafkaSource<K, V> fromOffsets(KafkaMessageIdList consumedOffsets); 
        
        
        SingleKafkaSource<K, V> filter(KafkaMessageIdList messageIds);
    }
    
    
    
    
    private static class SingleKafkaSourceImpl<K, V> implements SingleKafkaSource<K, V> {
        
        private final ImmutableMap<String, Object> properties;
        private final String topic; 
        private final KafkaMessageIdList consumedOffsets;
        private final KafkaMessageIdList idsToFilter;



        private SingleKafkaSourceImpl(ImmutableMap<String, Object> properties, 
                                      String topic, 
                                      KafkaMessageIdList consumedOffsets,
                                      KafkaMessageIdList idsToFilter) {
            this.properties = properties;
            this.topic = topic;
            this.consumedOffsets = consumedOffsets;
            this.idsToFilter = idsToFilter;
        }

        
        
        @Override
        public SingleKafkaSourceImpl<K, V> fromOffsets(KafkaMessageIdList consumedOffsets) {
            return new SingleKafkaSourceImpl<>(this.properties,         
                                               this.topic, 
                                               consumedOffsets,
                                               this.idsToFilter);
        }

        
        @Override
        public SingleKafkaSourceImpl<K, V> filter(KafkaMessageIdList idsToFilter) {
            return new SingleKafkaSourceImpl<>(this.properties,         
                                               this.topic, 
                                               this.consumedOffsets,
                                               idsToFilter);
        }
        
        
        @Override
        public void subscribe(Subscriber<? super KafkaMessage<K, V>> subscriber) {
            // https://github.com/reactive-streams/reactive-streams-jvm#1.9
            if (subscriber == null) {  
                throw new NullPointerException("subscriber is null");
            }
            
            new ConsumerSubscription<K, V>(topic, properties, consumedOffsets, idsToFilter, subscriber);
        }
        
        
        
        private static class ConsumerSubscription<K, V> implements Subscription {
            
            private final SubscriberNotifier<KafkaMessage<K, V>> subscriberNotifier; 
            private final AtomicBoolean isOpen = new AtomicBoolean(true);
            private final InboundBuffer inboundBuffer;

            
            private ConsumerSubscription(String topic, 
                                         ImmutableMap<String, Object> properties, 
                                         KafkaMessageIdList consumedOffsets,
                                         KafkaMessageIdList idsToFilter,
                                         Subscriber<? super KafkaMessage<K, V>> subscriber) {

                this.subscriberNotifier = new SubscriberNotifier<>(subscriber, this);

                
                KafkaConsumer<K, V> consumer = new KafkaConsumer<>(ImmutableMap.copyOf(properties));
            
            
                
                // seek to offset
                ImmutableList<TopicPartition> partitions = getTopicPartitions(topic, consumer);
                consumer.assign(partitions);
            
                for (TopicPartition partition : partitions) {
                    Optional<Long> offset = KafkaMessageId.getOffSet(consumedOffsets, partition.partition());
                    if (offset.isPresent()) {
                        consumer.seek(partition, offset.get() + 1);
                    } else {
                        consumer.seekToBeginning(partition);
                    }
                }
                 
                
                this.inboundBuffer = new InboundBuffer(consumedOffsets,
                                                       idsToFilter,
                                                       consumer, 
                                                       (record) -> subscriberNotifier.notifyOnNext(record));
                Thread t = new Thread(inboundBuffer);
                t.setDaemon(true);
                t.start(); 
                
                subscriberNotifier.start();
            }
            
            
            private static <K, V> ImmutableList<TopicPartition> getTopicPartitions(String topic, KafkaConsumer<K, V> consumer) {
                List<TopicPartition> partitions =  consumer.partitionsFor(topic)
                                                           .stream()
                                                           .map(partition -> new TopicPartition(topic, partition.partition()))
                                                           .collect(Collectors.toList());
                return ImmutableList.copyOf(partitions);
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
                private final Queue<KafkaMessage<K, V>> bufferedRecords = Queues.newConcurrentLinkedQueue();
                private final Consumer<KafkaMessage<K, V>> recordConsumer;
                
                private final KafkaConsumer<K, V> consumer;
                private final AtomicInteger numPendingRequested = new AtomicInteger(0);
                
                private final Object processingLock = new Object();
                private KafkaMessageIdList consumedOffsets;
                private Optional<KafkaMessageIdList> optionalIdsToFilter;

                
                
                public InboundBuffer(KafkaMessageIdList consumedOffsets,
                                     KafkaMessageIdList idsToFilter,
                                     KafkaConsumer<K, V> consumer, 
                                     Consumer<KafkaMessage<K, V>> recordConsumer) {
                    
                    this.consumedOffsets = consumedOffsets;
                    this.optionalIdsToFilter = idsToFilter.isEmpty() ? Optional.empty() : Optional.of(idsToFilter);
                    this.consumer = consumer;
                    this.recordConsumer = recordConsumer;
                }

                public void onRequested(int num) {
                    numPendingRequested.addAndGet(num);
                    process();
                }
                

                public void onRecords(ConsumerRecords<K, V> records) {
                    
                    for (ConsumerRecord<K, V> record : records) {
                        
                        if (optionalIdsToFilter.isPresent()) {
                            KafkaMessageId msgId = KafkaMessageId.valueOf(record.partition(), record.offset());
                            if (optionalIdsToFilter.get().contains(msgId)) {
                                optionalIdsToFilter = Optional.of(optionalIdsToFilter.get().erase(msgId));
                            } else {
                                continue;
                            }
                        }

                        KafkaMessageIdList newConsumedOffsets = consumedOffsets.replacePartitionOffset(record.partition(), record.offset());
                        this.consumedOffsets = newConsumedOffsets;
                        
                        KafkaMessage<K, V> msg = new KafkaMessage<>(newConsumedOffsets, record);
                        bufferedRecords.add(msg);
                    }
                    
                    process();
                }
                
                
                private void process() {
                    
                    while (numPendingRequested.get() > 0) {
                        
                        synchronized (processingLock) {
                            KafkaMessage<K, V> record = bufferedRecords.poll();
                            if (record == null) {
                                
                                if (optionalIdsToFilter.isPresent()) {
                                    if (optionalIdsToFilter.get().isEmpty()) {
                                        cancel();
                                    }
                                }
                                
                                return;
                            } else {
                                recordConsumer.accept(record);
                                numPendingRequested.decrementAndGet();
                            }
                        }
                    }
                }
                 
                
                @Override
                public void run() {
                    while (true) {
                        onRecords(consumer.poll(100));
                    }
                }
            }
        }
    }  
}  