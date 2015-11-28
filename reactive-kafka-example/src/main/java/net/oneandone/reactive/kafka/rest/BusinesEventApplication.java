package net.oneandone.reactive.kafka.rest;



import java.io.File;
import java.util.Map;

import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import net.oneandone.avro.json.JsonAvroMapperRegistry;
import net.oneandone.reactive.kafka.CompletableKafkaProducer;





@SpringBootApplication
public class BusinesEventApplication extends ResourceConfig {
    
    @Value("${eventbus.bootstrapservers}")
    private String bootstrapservers;

    @Value("${eventbus.zookeeper}")
    private String zookeeperConnect;

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
    public KafkaSourceFactory<String, byte[]> kafkaSourceFactory() {
        Map<String, Object> props = Maps.newHashMap();
        props.put("bootstrap.servers", bootstrapservers);
        props.put("zookeeper.connect", zookeeperConnect);
        props.put("key.deserializer", org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        props.put("value.deserializer", org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
        
        return new KafkaSourceFactory<String, byte[]>(ImmutableMap.copyOf(props));
    }
    
    
 
    
    
    @Bean
    public JsonAvroMapperRegistry jsonAvroMapperRegistry() {
        return new JsonAvroMapperRegistry(new File(schemaRegistryPath));
    }
    
    
    
    public static void main(String[] args) {
        SpringApplication.run(BusinesEventApplication.class, args);
    }

}