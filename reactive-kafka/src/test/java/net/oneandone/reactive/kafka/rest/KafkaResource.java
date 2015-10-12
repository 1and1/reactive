package net.oneandone.reactive.kafka.rest;


import java.io.Closeable;
import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.collect.ImmutableMap;

import net.oneandone.reactive.kafka.CompletableKafkaProducer;
import net.oneandone.reactive.rest.container.ResultConsumer;





@Path("/")
public class KafkaResource implements Closeable {
    private final CompletableKafkaProducer<String, String> kafkaProducer;
    
    
    public KafkaResource(String bootstrapservers) {
        this.kafkaProducer = new CompletableKafkaProducer<>(ImmutableMap.of("bootstrap.servers", bootstrapservers,
                                                                            "key.serializer", org.apache.kafka.common.serialization.StringSerializer.class,
                                                                            "value.serializer", org.apache.kafka.common.serialization.StringSerializer.class));
    }
    
    @Override
    public void close() throws IOException {
        kafkaProducer.close();
    }
    
    
    
    @POST
    @Path("/topics/{topic}")
    public void cunsume(@Context UriInfo uriInfo,
                        @PathParam("topic") String topic,
                        @HeaderParam("Content-Type") String contentType, 
                        String data,
                        @Suspended AsyncResponse response) {

        final UriBuilder uriBuilder = uriInfo.getAbsolutePathBuilder();
        
        
        // the kafka message consists of a header followed by a blank line and the body. E.G.
        // 
        // ---
        // Content-Type: application/vnd.ui.events.user.addressmodified-v1+json
        //
        // {"addressChangedEvent":{"datetime":"2015-10-12T05:00:18.613Z","accountId":"us-r3344434","address":"myAddress","operation":"add"}}
        // ---
        //
        final String kafkaMessage = "Content-Type: " + contentType + "\r\n" +
                                    "\r\n" + 
                                    data;  

        
        // send the kafka message in an asynchronous way 
        kafkaProducer.sendAsync(new ProducerRecord<String, String>(topic, kafkaMessage))
                     .thenApply(metadata -> Response.created(uriBuilder.path("partition")
                                                                       .path(Integer.toString(metadata.partition()))
                                                                       .path("offset")
                                                                       .path(Long.toString(metadata.offset()))
                                                                       .build()).build()) 
                     .whenComplete(ResultConsumer.writeTo(response));
    }
    
    
    
    @GET
    @Path("/topics/{topic}/partition/{partition}/offset/{offset}")
    public void produce(@PathParam("topic") String topic,
                        @PathParam("partition") String partition,
                        @PathParam("offset") String offset,
                        @Suspended AsyncResponse response) {
        
        // To be implemented
    }
    
    

    @GET
    @Path("/topics/{topic}")
    @Produces("text/event-stream")
    public void produceStream(@PathParam("topic") String topic,
                              @HeaderParam("Last-Event-Id") String lastEventId,
                              @Suspended AsyncResponse response) {
        
        // starts reading the stream base on the last event id (if present)
        // To be implemented
    }
}