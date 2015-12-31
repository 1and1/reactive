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


import java.net.URI;

import java.util.Map;

import javax.ws.rs.ApplicationPath;

import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import net.oneandone.commons.incubator.hammer.problem.StdProblem;
import net.oneandone.commons.incubator.neo.freemarker.FreemarkerProvider;
import net.oneandone.commons.incubator.neo.problem.GenericExceptionMapper;
import net.oneandone.reactive.kafka.CompletableKafkaProducer;
import net.oneandone.reactive.kafka.KafkaSource;
import net.oneandone.reactive.kafka.avro.json.AvroMessageMapperRepository;
import net.oneandone.reactive.kafka.avro.json.AvroSerializationException;
import net.oneandone.reactive.kafka.avro.json.SchemaException;





@ApplicationPath("/rest")
@SpringBootApplication
public class BusinesEventApplication extends ResourceConfig {

    @Value("${eventbus.bootstrapservers}")
    private String bootstrapservers;

    @Value("${eventbus.zookeeper}")
    private String zookeeperConnect;

    @Value("${schemaregistry.schema.uri}")
    private String schemasUri;

    
    
    
    public static void main(final String[] args) {
        SpringApplication.run(BusinesEventApplication.class, args);
    }

    
    
    ///////////////////////////////////////////
    // JAX-RS config 
    
    public BusinesEventApplication() {
        register(BusinesEventResource.class);
        register(FreemarkerProvider.class);
        register(new GenericExceptionMapper().withProblemMapper(AvroSerializationException.class, e -> StdProblem.newMalformedRequestDataProblem())
                                             .withProblemMapper(SchemaException.class, "POST", "PUT", e -> StdProblem.newUnsupportedMimeTypeProblem().withParam("type", e.getType()))
                                             .withProblemMapper(SchemaException.class, "GET", e -> StdProblem.newUnacceptedMimeTypeProblem().withParam("type", e.getType())));
        
        System.setProperty("javax.ws.rs.client.ClientBuilder", net.oneandone.commons.incubator.hammer.http.client.RestClientBuilder.class.getName());
    } 
    
    //
    ////////////////////////////////////////////////


    
    
    
    
    ///////////////////////////////////////////
    // WIRING APPLICATION CLASSES


    @Bean(destroyMethod="close")
    public CompletableKafkaProducer<String, byte[]> kafkaProducer() {
        return new CompletableKafkaProducer<>(ImmutableMap.of("bootstrap.servers", bootstrapservers,
                                                              "key.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class,
                                                              "value.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class));
    }
    
    
    
    @Bean
    public KafkaSource<String, byte[]> kafkaSource() {
        Map<String, Object> props = Maps.newHashMap();
        props.put("bootstrap.servers", bootstrapservers);
        props.put("zookeeper.connect", zookeeperConnect);
        props.put("key.deserializer", org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        props.put("value.deserializer", org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        
        return new KafkaSource<String, byte[]>(ImmutableMap.copyOf(props));
    }
    
    
    
    @Bean(destroyMethod="close")
    public AvroMessageMapperRepository avroMessageMapperRepository() {
        return new AvroMessageMapperRepository(URI.create(schemasUri));
    }
    
    //
    ///////////////////////////////////////////////////
}