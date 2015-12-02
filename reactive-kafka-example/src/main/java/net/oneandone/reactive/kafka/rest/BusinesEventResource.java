package net.oneandone.reactive.kafka.rest;

import java.io.IOException;


import java.io.InputStream;



import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import jersey.repackaged.com.google.common.collect.Lists;
import net.oneandone.avro.json.JsonAvroMapperRegistry;
import net.oneandone.avro.json.SchemaException;
import net.oneandone.avro.json.AvroMessageSerializer;
import net.oneandone.avro.json.JsonAvroMapper;
import net.oneandone.reactive.ReactiveSource;
import net.oneandone.reactive.kafka.CompletableKafkaProducer;
import net.oneandone.reactive.kafka.KafkaMessage;
import net.oneandone.reactive.kafka.KafkaMessageId;
import net.oneandone.reactive.kafka.KafkaSource;
import net.oneandone.reactive.rest.container.ResultConsumer;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.servlet.ServletSseSubscriber;
import net.oneandone.reactive.utils.Pair;
import rx.Observable;
import rx.RxReactiveStreams;





@Path("/")
public class BusinesEventResource {
    
    private final JsonAvroMapperRegistry jsonAvroMapperRegistry; 
    private final CompletableKafkaProducer<String, byte[]> kafkaProducer;
    private final KafkaSource<String, byte[]> kafkaSourcePrototype;
    

    @Autowired
    public BusinesEventResource(CompletableKafkaProducer<String, byte[]> kafkaProducer,
                                KafkaSource<String, byte[]> kafkaSourcePrototype,
                                JsonAvroMapperRegistry jsonAvroMapperRegistry) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaSourcePrototype = kafkaSourcePrototype;
        this.jsonAvroMapperRegistry = jsonAvroMapperRegistry;
    }
    

    
    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Object getRoot(@Context UriInfo uriInfo) {
        return ImmutableMap.of("_links", LinksBuilder.create(uriInfo).withRelativeHref("topics").build());
    }
    
    
    @GET
    @Path("/topics")
    @Produces("application/vnd.ui.mam.eventservice.topic.list+json")
    public TopicsRepresentation getTopics(@Context UriInfo uriInfo, @QueryParam("q.topic.name.eq") String topicname) {
        return new TopicsRepresentation(uriInfo, "topics", ImmutableList.of(getTopic(uriInfo, topicname))); 
    }
    
    
    
    @GET
    @Path("/topics/{topicname}")
    @Produces("application/vnd.ui.mam.eventservice.topic+json")
    public TopicRepresentation getTopic(@Context UriInfo uriInfo, @PathParam("topicname") String topicname) {
        return new TopicRepresentation(uriInfo, "topics", topicname);
    }
        
  

    @GET
    @Path("/topics/{topic}/schemas")
    @Produces(MediaType.TEXT_PLAIN)
    public String getRegisteredSchematas() {
        
        final StringBuilder builder = new StringBuilder();
        for (Entry<String, JsonAvroMapper> entry : jsonAvroMapperRegistry.getRegisteredMapper().entrySet()) {
            builder.append("== " + entry.getKey() + " ==\r\n");
            builder.append(entry.getValue() + "\r\n\r\n\r\n");
        }
        return builder.toString();
    }
    
    
    
    @POST
    @Path("/topics/{topic}/events")
    public void consume(@Context UriInfo uriInfo,
                        @PathParam("topic") String topicname,
                        @HeaderParam("Content-Type") String contentType, 
                        InputStream jsonObjectStream,
                        @Suspended AsyncResponse response) throws BadRequestException {

         
        final ImmutableList<byte[]> binaryAvroMessages = jsonAvroMapperRegistry.getJsonToAvroMapper(contentType)
                                                                               .map(mapper -> mapper.toAvroRecords(jsonObjectStream))
                                                                               .map(avroMessages -> AvroMessageSerializer.serialize(avroMessages))
                                                                               .orElseThrow(BadRequestException::new);  
        
        
        final String topicPath = uriInfo.getBaseUriBuilder().path("topics").path(topicname).toString();
        
        sendAsync(topicname, binaryAvroMessages, ImmutableList.of())
                .thenApply(ids -> contentType.toLowerCase(Locale.US).endsWith(".list+json") ? (topicPath + "/eventcollections/" + KafkaMessageId.toString(ids)) 
                                                                                            : (topicPath + "/events/" + ids.get(0).toString()))
                .thenApply(uri -> Response.created(URI.create(uri)).build())
                .whenComplete(ResultConsumer.writeTo(response));
    }
    
         
    
    private CompletableFuture<ImmutableList<KafkaMessageId>> sendAsync(String topic, ImmutableList<byte[]> kafkaMessages, ImmutableList<KafkaMessageId> sentIds) {
        
        if (kafkaMessages.isEmpty()) {
            return CompletableFuture.completedFuture(sentIds);
            
        } else {
            return kafkaProducer.sendAsync(new ProducerRecord<String, byte[]>(topic, kafkaMessages.get(0)))
                                .thenApply(metadata -> ImmutableList.<KafkaMessageId>builder()
                                                                    .addAll(sentIds)
                                                                    .add(new KafkaMessageId(metadata.partition(), metadata.offset())).build())
                                .thenCompose(newSentIds -> sendAsync(topic, kafkaMessages.subList(1,  kafkaMessages.size()), newSentIds));
        }
    }
    

    
    @GET
    @Path("/topics/{topic}/events/{id}")
    public void produce(@PathParam("topic") String topic,
                        @PathParam("id") KafkaMessageId id,
                        @HeaderParam("Accept") @DefaultValue("*/*") MediaType readerMimeType,
                        @Suspended AsyncResponse response) throws IOException {
        
        
        try (ReactiveSource<KafkaMessage<String, byte[]>> reactiveSource = kafkaSourcePrototype.withTopic(topic)
                                                                                               .filter(ImmutableList.of(id))
                                                                                               .open()) {
            
            KafkaMessage<String, byte[]> msg = reactiveSource.read(Duration.ofSeconds(3));
            
            final Schema readerSchema = isApplicationWildcardType(readerMimeType) ? null
                                                                                  : jsonAvroMapperRegistry.getJsonToAvroMapper(readerMimeType.toString())
                                                                                                         .orElseThrow(BadRequestException::new)
                                                                                                         .getSchema();
            
            GenericRecord avroMessage = AvroMessageSerializer.deserialize(msg.value(), jsonAvroMapperRegistry, readerSchema);
            
            
            JsonAvroMapper mapper = jsonAvroMapperRegistry.getJsonToAvroMapper(avroMessage.getSchema())
                                                          .orElseThrow(SchemaException::new);
            
            response.resume(Response.ok(mapper.toBinaryJson(avroMessage))
                                    .type(mapper.getMimeType()).build());
        }
    }
    
    
    
    private boolean isApplicationWildcardType(MediaType readerMimeType) {
        return (readerMimeType.isWildcardType() || (readerMimeType.getType().equalsIgnoreCase("application") && readerMimeType.isWildcardSubtype()));
    }

    
    
    @GET
    @Path("/topics/{topic}/eventcollections/{ids}")
    public void produceView(@PathParam("topic") String topic,
                            @PathParam("ids") String ids,
                            @HeaderParam("Accept") @DefaultValue("*/*") MediaType readerMimeType,
                            @Suspended AsyncResponse response) throws IOException {
        
        
        try (ReactiveSource<KafkaMessage<String, byte[]>> reactiveSource = kafkaSourcePrototype.withTopic(topic)
                                                                                               .filter(KafkaMessageId.valuesOf(ids))
                                                                                               .open()) {

            JsonArrayBuilder builder = Json.createArrayBuilder();
            
            List<KafkaMessageId> identifiers = Lists.newArrayList(KafkaMessageId.valuesOf(ids));

            String mimetype = MediaType.APPLICATION_JSON;
            
            while (!identifiers.isEmpty()) {
                KafkaMessage<String, byte[]> msg = reactiveSource.read(Duration.ofSeconds(3));
                
                final Schema readerSchema = (readerMimeType.isWildcardType() || (readerMimeType.getType().equalsIgnoreCase("application") && readerMimeType.isWildcardSubtype())) ? null
                                                                     : jsonAvroMapperRegistry.getJsonToAvroMapper(readerMimeType.toString())
                                                                                             .orElseThrow(BadRequestException::new)
                                                                                             .getSchema();
                
                GenericRecord avroMessage = AvroMessageSerializer.deserialize(msg.value(), jsonAvroMapperRegistry, readerSchema);
                
                
                KafkaMessageId id = KafkaMessageId.valueOf(msg.partition(), msg.offset());
                if (identifiers.contains(id)) {
                    JsonAvroMapper mapper = jsonAvroMapperRegistry.getJsonToAvroMapper(avroMessage.getSchema())
                                                                  .orElseThrow(SchemaException::new);
                    JsonObject json = mapper.toJson(avroMessage);
                    builder.add(json);
                    
                    identifiers.remove(KafkaMessageId.valueOf(msg.partition(), msg.offset()));
                    mimetype = mapper.getMimeType();
                }                
            }

            
            JsonArray jsonArray = builder.build();
            response.resume(Response.ok(jsonArray.toString().getBytes(Charsets.UTF_8)).type(mimetype).build());
        }
    }
    
    

    @GET
    @Path("/topics/{topic}/events")
    @Produces("text/event-stream")
    public void produceReactiveStream(@PathParam("topic") String topic,
                                      @HeaderParam("Last-Event-Id") String consumedOffsets,
                                      @QueryParam("q.event.eq") String acceptedEventtype, 
                                      @Context HttpServletRequest req,
                                      @Context HttpServletResponse resp,
                                      @Suspended AsyncResponse response) {

        resp.setContentType("text/event-stream");

        
        // compose greeting message 
        String prologComment = "stream opened. emitting " + ((acceptedEventtype == null) ? "all event types" : acceptedEventtype + " event types only");
        prologComment = (consumedOffsets == null) ? prologComment : prologComment + " with offset id " + KafkaMessageId.valuesOf(consumedOffsets); 

        
        Predicate<GenericRecord> filter = FilterCondition.valueOf(ImmutableMap.copyOf(req.getParameterMap()));
        final Schema readerSchema = (acceptedEventtype == null) ? null : jsonAvroMapperRegistry.getJsonToAvroMapper(acceptedEventtype)
                                                                                               .orElseThrow(BadRequestException::new)
                                                                                               .getSchema();   
        
        // establish reactive response stream 
        Observable<ServerSentEvent> obs = RxReactiveStreams.toObservable(kafkaSourcePrototype.withTopic(topic).fromOffsets(KafkaMessageId.valuesOf(consumedOffsets)))
                                                           .map(message -> Pair.of(message.getConsumedOffsets(), AvroMessageSerializer.deserialize(message.value(), jsonAvroMapperRegistry, readerSchema)))
                                                           .filter(pair -> filter.test(pair.getSecond()))
                                                           .map(idMsgPair -> toServerSentEvent(idMsgPair.getFirst(), idMsgPair.getSecond()));
        RxReactiveStreams.toPublisher(obs).subscribe(new ServletSseSubscriber(req, resp, prologComment));
    }
    
    
   
    private ServerSentEvent toServerSentEvent(ImmutableList<KafkaMessageId> consumedOffsets, GenericRecord avroMessage) {
        JsonAvroMapper mapper = jsonAvroMapperRegistry.getJsonToAvroMapper(avroMessage.getSchema())
                                                      .orElseThrow(SchemaException::new);
        
        return ServerSentEvent.newEvent().id(KafkaMessageId.toString(consumedOffsets))
                                         .event(mapper.getMimeType())
                                         .data(mapper.toJson(avroMessage).toString());
    }
    
   
    
    
    private static final class FilterCondition {

        
        public static Predicate<GenericRecord> valueOf(ImmutableMap<String, String[]> filterParams) {

            Predicate<GenericRecord> filter = (record) -> true;
            
            
            for (Entry<String, String[]> entry : filterParams.entrySet()) {
                if (entry.getKey().startsWith("q.data.")) {
                    for (String value : entry.getValue()) {
                        
                        filter = filter.and(newFilterCondition(entry.getKey(), value));
                    }
                }
            }
            
            return filter;
        }
        
        
        private static final Predicate<GenericRecord> newFilterCondition(String condition, String value) {
            String name = condition.substring("q.data.".length(), condition.lastIndexOf("."));
            
            if (condition.endsWith(".eq")) {
                return (record) -> read(name, record).map(obj -> obj.toString().equals(value))
                                                     .orElse(false);
                
            } else if (condition.endsWith(".ne")) {
                return (record) -> read(name, record).map(obj -> !obj.toString().equals(value))
                                                     .orElse(false);

            } else if (condition.endsWith(".in")) {
                ImmutableList<String> values = ImmutableList.copyOf(Splitter.on(",").trimResults().splitToList(value));
                return (record) -> read(name, record).map(obj -> values.contains(obj.toString()))
                                                     .orElse(false);

            } else if (condition.endsWith(".gt")) {
                return (record) -> read(name, record).map(obj -> obj.toString().compareTo(value) > 0)
                                                     .orElse(false);
                
            } else if (condition.endsWith(".lt")) {
                return (record) -> read(name, record).map(obj -> obj.toString().compareTo(value) < 0)
                                                     .orElse(false);

            } else {
                return (record) -> true;
            }
        }
        
        
        private static Optional<Object> read(String dotSeparatedName, GenericRecord record) {
            Optional<Object> result = Optional.empty();
            
            for (String namePart : Splitter.on(".").trimResults().splitToList(dotSeparatedName)) {
                Object obj = record.get(namePart);
                if (obj == null) {
                    break;
                } else {
                    if (obj instanceof Record) {
                        record = (Record) obj;
                    } else {
                        result = Optional.of(obj);
                    }
                }
            }
            
            return result;
        }
    }    
}