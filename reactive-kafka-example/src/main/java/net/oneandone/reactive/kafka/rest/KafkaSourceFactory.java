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


import com.google.common.collect.ImmutableMap;



public class KafkaSourceFactory<K, V> {

    private final ImmutableMap<String, Object> props;
    
    public KafkaSourceFactory(ImmutableMap<String, Object> props) {
        this.props = props;
    }
    
    
    public KafkaSource<K, V> newKafkaSource(String topic) {
        return new KafkaSource<K, V>(topic, props);
    }
}  