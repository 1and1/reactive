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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;



public class CompletableKafkaProducer<K, V> extends KafkaProducer<K, V> {
    
    private static final Logger LOG = LoggerFactory.getLogger(CompletableKafkaProducer.class);
    

    public CompletableKafkaProducer(ImmutableMap<String, Object> properties) {
        super(properties);
    }

 
    
    public CompletableFuture<RecordMetadata> sendAsync(ProducerRecord<K, V> record) {
        
        final CompletableFuture<RecordMetadata> promise = new CompletableFuture<>();
        
        
        final Callback callback = new Callback() {
            
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    LOG.debug("submit into " + metadata.topic() + "  (" + metadata.offset() + "@" + metadata.partition() + ")");
                    promise.complete(metadata);
                } else {
                    LOG.debug(exception.getMessage() + " error occured by writing data");
                    promise.completeExceptionally(exception);
                }
            }
        };

        super.send(record, callback);
        
        
        return promise;
    }
    
}