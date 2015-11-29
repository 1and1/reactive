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



import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;




public class KafkaSource<K, V> implements Publisher<KafkaSource.TopicMessage<K, V>> {
    
    // properties
    private final ImmutableMap<String, Object> properties;
    private final ConsumedOffsets consumedOffsets;
    private final ImmutableList<String> topics; 

    
    
    public KafkaSource(ImmutableMap<String, Object> properties) {
        this(ImmutableList.of(), properties, ConsumedOffsets.valueOf(""));
    }


    private KafkaSource(ImmutableList<String> topics,
                        ImmutableMap<String, Object> properties,
                        ConsumedOffsets consumedOffsets) {
        this.topics = topics;
        this.properties = properties;
        this.consumedOffsets = consumedOffsets;
    }


    public KafkaSource<K, V> withTopic(String topic) {
        return new KafkaSource<>(ImmutableList.<String>builder().addAll(topics).add(topic).build(),
                                 this.properties,
                                 this.consumedOffsets);
    }
    
    
    
    public KafkaSource<K, V> withProperty(String name, Object value) {
        return new KafkaSource<>(this.topics,
                                 ImmutableMap.<String, Object>builder().putAll(properties).put(name, value).build(),
                                 this.consumedOffsets);
    }

    
    public KafkaSource<K, V> withConsumedOffsets(ConsumedOffsets consumedOffsets) {
        return new KafkaSource<>(this.topics,
                                 this.properties,
                                 consumedOffsets);
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
        
        new ConsumerSubscription<K, V>(topics, properties, consumedOffsets, subscriber);
    }
    
    
    
    private static class ConsumerSubscription<K, V> implements Subscription {
        
        private final SubscriberNotifier<KafkaSource.TopicMessage<K, V>> subscriberNotifier; 
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        
        private final InboundBuffer inboundBuffer;

        
        private ConsumerSubscription(ImmutableList<String> topics, 
                                     ImmutableMap<String, Object> properties, 
                                     ConsumedOffsets consumedOffsets,
                                     Subscriber<? super KafkaSource.TopicMessage<K, V>> subscriber) {

            this.subscriberNotifier = new SubscriberNotifier<>(subscriber, this);

            
            KafkaConsumer<K, V> consumer = new KafkaConsumer<>(ImmutableMap.copyOf(properties));
        
            
            ImmutableList<TopicPartition> partitions = getTopicPartitions(topics, consumer);
            consumer.assign(partitions);
        
            for (TopicPartition partition : partitions) {
                Optional<Long> offset = consumedOffsets.getOffSet(partition.topic(), partition.partition());
                if (offset.isPresent()) {
                    consumer.seek(partition, offset.get() + 1);
                } else {
                    consumer.seekToBeginning(partition);
                }
            }
             
            
            this.inboundBuffer = new InboundBuffer(consumedOffsets, consumer, (record) -> subscriberNotifier.notifyOnNext(record));
            Thread t = new Thread(inboundBuffer);
            t.setDaemon(true);
            t.start(); 
            
            subscriberNotifier.start();
        }
        
        
        private static <K, V> ImmutableList<TopicPartition> getTopicPartitions(ImmutableList<String> topics, KafkaConsumer<K, V> consumer) {
            List<TopicPartition> partitions = Lists.newArrayList();
            for (String topic : topics) {
                partitions.addAll(consumer.partitionsFor(topic)
                                          .stream()
                                          .map(partition -> new TopicPartition(topic, partition.partition()))
                                          .collect(Collectors.toList()));
            };

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
            private final Queue<KafkaSource.TopicMessage<K, V>> bufferedRecords = Queues.newConcurrentLinkedQueue();
            private final Consumer<KafkaSource.TopicMessage<K, V>> recordConsumer;
            
            private final KafkaConsumer<K, V> consumer;

            private final AtomicReference<ConsumedOffsets> consumedOffsets;
            
            private final AtomicInteger numPendingRequested = new AtomicInteger(0);

            
            public InboundBuffer(ConsumedOffsets consumedOffsets, KafkaConsumer<K, V> consumer, Consumer<KafkaSource.TopicMessage<K, V>> recordConsumer) {
                this.consumedOffsets = new AtomicReference<>(consumedOffsets);
                this.consumer = consumer;
                this.recordConsumer = recordConsumer;
            }

            public void onRequested(int num) {
                numPendingRequested.addAndGet(num);
                process();
            }
            

            public void onRecords(ConsumerRecords<K, V> records) {
                
                for (ConsumerRecord<K, V> record : records) {
                    ConsumedOffsets newConsumedOffsets = consumedOffsets.get().withOffset(record.topic(), record.partition(), record.offset());
                    this.consumedOffsets.set(newConsumedOffsets);
                    bufferedRecords.add(new TopicMessage<>(newConsumedOffsets, record));
                }
                
                process();
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
        
        private final ConsumedOffsets consumedOffsets;
        private final ConsumerRecord<K, V> record;
        
        public TopicMessage(ConsumedOffsets consumedOffsets, ConsumerRecord<K, V> record) {
            this.consumedOffsets = consumedOffsets;
            this.record = record;
        }

        public ConsumedOffsets getConsumedOffsets() {
            return consumedOffsets;
        }

        public ConsumerRecord<K, V> getRecord() {
            return record;
        }
    }
    
    
    

    public static final class ConsumedOffsets {
        private final ImmutableMap<String, ImmutableMap<Integer, Long>> consumedOffsets;
        
        private ConsumedOffsets(ImmutableMap<String, ImmutableMap<Integer, Long>> consumedOffsets) {
            this.consumedOffsets = consumedOffsets;
        }

        
        public static final ConsumedOffsets valueOf(String id) {

            if (Strings.isNullOrEmpty(id)) {
                return new ConsumedOffsets(ImmutableMap.of());
                
            } else {
                String txt = new String(Base64.getUrlDecoder().decode(id), Charsets.UTF_8);
                
                Map<String, ImmutableMap<Integer, Long>> result = Maps.newHashMap();
                for (Entry<String, String> entry : Splitter.on("&").withKeyValueSeparator(":").split(txt).entrySet()) {
                    result.put(entry.getKey(), ImmutableMap.copyOf(Splitter.on("#").withKeyValueSeparator("=").split(entry.getValue())
                                                                                                              .entrySet()
                                                                                                              .stream()
                                                                                                              .collect(Collectors.toMap(k -> Integer.parseInt(k.getKey()),
                                                                                                                                        v -> Long.parseLong(v.getValue())))));
                }
                
                return new ConsumedOffsets(ImmutableMap.copyOf(result));    
            }
        }
        
        
        public static final ConsumedOffsets of(ImmutableMap<String, ImmutableMap<Integer, Long>> consumedOffsets) {
            return new ConsumedOffsets(consumedOffsets);
        }
        
        
        public ImmutableMap<String, ImmutableMap<Integer, Long>> getConsumedOffsets() {
            return consumedOffsets;
        }
        
        
        public Optional<Long> getOffSet(String topic, int partition) {
            ImmutableMap<Integer, Long> partitionOffsets = consumedOffsets.get(topic);
            if (partitionOffsets != null) {
                Long offset = partitionOffsets.get(partition);
                if (offset != null) {
                    return Optional.of(offset);
                }
            }
            
            return Optional.empty();
        }
        
        
        public ConsumedOffsets withOffset(String topic, int partition, long offset) {
            ImmutableMap<Integer, Long> partitionOffsets = consumedOffsets.get(topic);
            if (partitionOffsets == null) {
                partitionOffsets = ImmutableMap.of(partition, offset);
            } else {
                Map<Integer, Long> map = Maps.newHashMap(partitionOffsets);
                map.put(partition, offset);
                partitionOffsets = ImmutableMap.copyOf(map);
            }
            
            
            Map<String, ImmutableMap<Integer, Long>> map = Maps.newHashMap(consumedOffsets);
            map.put(topic, partitionOffsets);
            return new ConsumedOffsets(ImmutableMap.copyOf(map));
        }
        
        
        @Override
        public String toString() {
            Map<String, String> topicOffsets = Maps.newHashMap();
            for (Entry<String, ImmutableMap<Integer, Long>> entry : consumedOffsets.entrySet()) {
                topicOffsets.put(entry.getKey(), Joiner.on("#")
                                                       .withKeyValueSeparator("=")
                                                       .join(entry.getValue()));
            }

            String txt = Joiner.on("&").withKeyValueSeparator(":").join(topicOffsets);
            return Base64.getUrlEncoder().encodeToString(txt.getBytes(Charsets.UTF_8));
        }
    }
}  