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



import org.apache.kafka.clients.consumer.ConsumerRecord;



 
public class KafkaMessage<K, V> {
        
    private final KafkaMessageIdList consumedOffsets;
    private final ConsumerRecord<K, V> record;
    
    public KafkaMessage(KafkaMessageIdList consumedOffsets, ConsumerRecord<K, V> record) {
        this.consumedOffsets = consumedOffsets;
        this.record = record;
    }

    public KafkaMessageIdList getConsumedOffsets() {
        return consumedOffsets;
    }
    
    
    public KafkaMessageId getKafkaMessageId() {
        return KafkaMessageId.valueOf(partition(), offset());
    }

    
    /**
     * The topic this record is received from
     */
    public String topic() {
        return record.topic();
    }

    /**
     * The partition from which this record is received
     */
    public int partition() {
        return record.partition();
    }

    /**
     * The key (or null if no key is specified)
     */
    public K key() {
        return record.key();
    }

    /**
     * The value
     */
    public V value() {
        return record.value();
    }

    /**
     * The position of this record in the corresponding Kafka partition.
     */
    public long offset() {
        return record.offset();
    }
    
    @Override
    public String toString() {
        return consumedOffsets.toString();
    }
}
