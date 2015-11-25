package net.oneandone.reactive.kafka.rest;



import java.io.File;

import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.google.common.collect.ImmutableMap;

import net.oneandone.avro.json.schemaregistry.AvroSchemaRegistry;
import net.oneandone.reactive.kafka.CompletableKafkaProducer;





@SpringBootApplication
public class BusinesEventApplication extends ResourceConfig {
    
    @Value("${eventbus.bootstrapservers}")
    private String bootstrapservers;
    
    @Value("${schemaregistry.path}")
    private String schemaRegistryPath;
    
    
    
    public BusinesEventApplication() {
        register(BusinesEventResource.class);
    } 


    @Bean(destroyMethod="close")
    public CompletableKafkaProducer<String, byte[]> kafkaProducer() {
        return new CompletableKafkaProducer<>(ImmutableMap.of("bootstrap.servers", bootstrapservers,
                                                              "key.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class,
                                                              "value.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class));
    }
    
    @Bean
    public AvroSchemaRegistry avroSchemaRegistry() {
        return new AvroSchemaRegistry(new File(schemaRegistryPath));
    }
    
    
    
    public static void main(String[] args) {
        SpringApplication.run(BusinesEventApplication.class, args);
    }

}