package net.oneandone.reactive.kafka.rest;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.BadRequestException;
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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;


import net.oneandone.reactive.kafka.CompletableKafkaProducer;
import net.oneandone.reactive.rest.container.ResultConsumer;


@Path("/avro")
public class KafkaResourceAvro implements Closeable {
    
    private final AvroMessageMapper avroMessageMapper = new AvroMessageMapper(); 
    private final CompletableKafkaProducer<String, byte[]> kafkaProducer;
    
    
    public KafkaResourceAvro(String bootstrapservers) {
        this.kafkaProducer = new CompletableKafkaProducer<>(ImmutableMap.of("bootstrap.servers", bootstrapservers,
                                                                            "key.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class,
                                                                            "value.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class));
    }
    
    @Override
    public void close() throws IOException {
        kafkaProducer.close();
    }
    
    
    
    @POST
    @Path("/topics/{topic}")
    public void consume(@Context UriInfo uriInfo,
                        @PathParam("topic") String topic,
                        @HeaderParam("Content-Type") String contentType, 
                        String jsonObject,
                        @Suspended AsyncResponse response) throws BadRequestException {

        final UriBuilder uriBuilder = uriInfo.getAbsolutePathBuilder();
        

        final byte[] kafkaMessage = avroMessageMapper.fromJson(contentType, jsonObject)
                                                     .orElseThrow(BadRequestException::new);  
        
 
        kafkaProducer.sendAsync(new ProducerRecord<String, byte[]>(topic, kafkaMessage))
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
        response.resume("OK");
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
    
    
    
    
    
    
 
    private static final class AvroMessageMapper {
        
        private final AvroSchemaRegistry schemaRegistry = new AvroSchemaRegistry();
        
        
        public AvroMessageMapper() {
            schemaRegistry.reloadSchemadefintions(ImmutableSet.of("application_vnd.ui.events.user.addressmodified-v1+json.avsc"));
        }
        
        public Optional<byte[]> fromJson(String mimeType, String jsonObject) {
            return schemaRegistry.getJsonToAvroWriter(mimeType)
                                 .map(writer -> writer.write(jsonObject));
        }
        
        public String toJson(String mimeType, byte[] avroObject) {
            // To be implemented
            return null;
        }
        
        
        private static final class JsonToAvroWriter {
            private final Schema schema;
            private final GenericDatumWriter<Object> writer;
                        
            public JsonToAvroWriter(String schemaAsString) {
                 this(new Schema.Parser().parse(schemaAsString));
            }
            
            public JsonToAvroWriter(Schema schema) {
                this.schema = schema;
                this.writer = new GenericDatumWriter<Object>(schema);
            }
            
            
            public byte[] write(String jsonObject) {
                try {
                    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    
                    final Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new DataInputStream(new ByteArrayInputStream(jsonObject.getBytes(Charsets.UTF_8))));
                    final Object datum = new GenericDatumReader<Object>(schema).read(null, decoder);
                    
                    final Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                    writer.write(datum, encoder);
                    encoder.flush();
                   
                    return outputStream.toByteArray();
                    
                } catch (IOException ioe) {
                    throw new BadRequestException("schema conflict " + ioe);
                }

            }
        }
        
        
        
        

        // TODO replace this: managing the local schema registry should be replaced by a data-replicator based approach
        // * the schema definition files will by loaded over URI by the data replicator, periodically
        // * the mime type is extracted from the filename by using naming conventions  

        private static final class AvroSchemaRegistry {
            
            private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaRegistry.class);
            
            private volatile ImmutableMap<String, JsonToAvroWriter> jsonToAvroWriters = ImmutableMap.of();
            
            
            public void reloadSchemadefintions(ImmutableSet<String> schemafilenames) {
                
                Map<String, JsonToAvroWriter> newJsonToAvroWriters = Maps.newHashMap();
                
                for (String filename : schemafilenames) {
                    final String mimeType = filename.substring(0, filename.length() - ".avsc".length()).replace("_", "/");
                    
                    try {
                        newJsonToAvroWriters.put(mimeType, new JsonToAvroWriter(Resources.toString(Resources.getResource(filename), Charsets.UTF_8)));
                    } catch (IOException ioe) {
                        LOG.warn("error loading avro schema " + filename, ioe);
                    }
                }
                
                jsonToAvroWriters = ImmutableMap.copyOf(newJsonToAvroWriters);
            }
            
            
            public Optional<JsonToAvroWriter> getJsonToAvroWriter(String mimeType) {
                return Optional.ofNullable(jsonToAvroWriters.get(mimeType));
            }
        }
    }
}