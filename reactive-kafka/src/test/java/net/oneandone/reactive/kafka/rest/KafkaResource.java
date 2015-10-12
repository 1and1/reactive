package net.oneandone.reactive.kafka.rest;


import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URL;

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
import org.junit.Ignore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

import net.oneandone.reactive.kafka.CompletableKafkaProducer;
import net.oneandone.reactive.rest.container.ResultConsumer;




@Ignore
@Path("/")
public class KafkaResource implements Closeable {
    
    private final JsonValidator jsonValidator = new JsonValidator(); 
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
                        String jsonObject,
                        @Suspended AsyncResponse response) {

        final UriBuilder uriBuilder = uriInfo.getAbsolutePathBuilder();
        
        
        // validate json schema
        jsonValidator.validate(contentType, jsonObject);
        
 
        
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
                                    jsonObject;  

        
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
    
    
    
    
    private static final class JsonValidator {
        
        private final ImmutableMap<String, JsonSchema> schemaRegistry; 
        
        
        public JsonValidator() {

            // TODO replace this: managing the local schema registry should be replaced by a data-replicator based approach
            // * the schema definition files will by loaded over URI by the data replicator, periodically
            // * the mime type is extracted form the filename by usaingt naming conventions  

            try {
                final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
                
                final String filename = "application_vnd.ui.events.user.addressmodified-v1+json.json";
                final JsonSchema schema = factory.getJsonSchema(JsonLoader.fromString(Resources.toString(Resources.getResource(filename), Charsets.UTF_8)));

                String mimeType = filename.substring(0, filename.length() - ".json".length()).replace("_", "/"); 
                schemaRegistry = ImmutableMap.of(mimeType, schema);
                
            } catch (ProcessingException | IOException e) {
                throw new RuntimeException(e);
            }      
                
        }
        
        
        public void validate(String mimeType, String jsonObject) {
            try {
                final ProcessingReport report = schemaRegistry.get(mimeType).validate(JsonLoader.fromString(jsonObject));
                if (report.isSuccess()) {
                    throw new IllegalArgumentException("schema conflict " + report.toString()); 
                }
            } catch (ProcessingException | IOException e) {
                throw new RuntimeException(e);
            }            
        }
    }
}