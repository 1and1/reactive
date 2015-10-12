package net.oneandone.reactive.kafka.rest;


import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import com.google.common.collect.ImmutableSet;




@ApplicationPath("/")
public class KafkaResourceApp extends Application {

    
    @Override
    public ImmutableSet<Object> getSingletons() {
        return ImmutableSet.of(new KafkaResource("localhost:" + 8553));
    }
}