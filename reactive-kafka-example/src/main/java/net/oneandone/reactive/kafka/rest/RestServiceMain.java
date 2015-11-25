package net.oneandone.reactive.kafka.rest;


import java.util.Arrays;

import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableMap;

import net.oneandone.avro.json.AvroSchemaRegistry;
import net.oneandone.reactive.kafka.CompletableKafkaProducer;





@Configuration
@EnableAutoConfiguration
@SpringBootApplication
public class RestServiceMain extends ResourceConfig {
    

    public static void main(String[] args) {

        ApplicationContext ctx = SpringApplication.run(RestServiceMain.class, args);
     
        System.out.println(ctx.getEnvironment().getProperty("server.port"));

        
        System.out.println("active profiles");
        for (String profile : ctx.getEnvironment().getActiveProfiles()) {
            System.out.println("profile " + profile);
        }
        
        System.out.println("default properties profiles");
        for (String prop : ctx.getEnvironment().getDefaultProfiles()) {
            System.out.println(prop + "=" + ctx.getEnvironment().getProperty(prop));
        }
        
        
        
        String[] beanNames = ctx.getBeanDefinitionNames();
        Arrays.sort(beanNames);
        for (String beanName : beanNames) {
            System.out.println(beanName);
        }
    }
    
    
    

    public RestServiceMain() {
        register(KafkaResource.class);
    } 


    @Bean(destroyMethod="close")
    public CompletableKafkaProducer<String, byte[]> kafkaProducer() {
        String bootstrapservers = "localhost:8553";
        return new CompletableKafkaProducer<>(ImmutableMap.of("bootstrap.servers", bootstrapservers,
                                                              "key.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class,
                                                              "value.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class));
    }
    
    @Bean
    public AvroSchemaRegistry avroSchemaRegistry() {
        return new AvroSchemaRegistry();
    }
}