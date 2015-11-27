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

import java.io.IOException;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;



public class EmbeddedKafka {
 
    private KafkaServerStartable kafka;
    private final int port;

    
    public EmbeddedKafka(ImmutableMap<String, String> kafkaProperties) throws IOException, InterruptedException{
        this.port = Integer.parseInt(kafkaProperties.get("port"));
        
        
        Properties props = new Properties();
        props.putAll(kafkaProperties);
        kafka = new KafkaServerStartable(new KafkaConfig(props));
    }
    
    public int getPort() {
        return port;
    }
    
    public void start() {
        kafka.startup();
    }
    
    public void shutdown(){
        kafka.shutdown();
    }
}



