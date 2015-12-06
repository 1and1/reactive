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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
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

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import net.oneandone.avro.json.AvroMessageMapperRepoistory;
import net.oneandone.avro.json.AvroMessage;
import net.oneandone.reactive.ReactiveSource;
import net.oneandone.reactive.kafka.CompletableKafkaProducer;
import net.oneandone.reactive.kafka.KafkaMessage;
import net.oneandone.reactive.kafka.KafkaMessageId;
import net.oneandone.reactive.kafka.KafkaSource;
import net.oneandone.reactive.rest.container.ResultConsumer;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.servlet.ServletSseSubscriber;
import net.oneandone.reactive.utils.LinksBuilder;
import net.oneandone.reactive.utils.Pair;
import rx.Observable;
import rx.RxReactiveStreams;




@Path("/")
public class BusinesEventResource {
    
    private final AvroMessageMapperRepoistory mapperRepository; 
    private final CompletableKafkaProducer<String, byte[]> kafkaProducer;
    private final KafkaSource<String, byte[]> kafkaSourcePrototype;

    

    @Autowired
    public BusinesEventResource(CompletableKafkaProducer<String, byte[]> kafkaProducer,
                                KafkaSource<String, byte[]> kafkaSourcePrototype,
                                AvroMessageMapperRepoistory avroMessageMapperRepository) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaSourcePrototype = kafkaSourcePrototype;
        this.mapperRepository = avroMessageMapperRepository;
    }
    

    
    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Object getRoot(@Context UriInfo uriInfo) {
        return ImmutableMap.of("_links", LinksBuilder.create(uriInfo).withHref("topics").build());
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
        return Joiner.on("\r\n").join(mapperRepository.getRegisteredSchemas()
                                                      .entrySet()
                                                      .stream()
                                                      .map(entry -> ("== " + entry.getKey() +  
                                                                     " ==\r\n" + entry.getValue() +
                                                                     "\r\n\r\n\r\n"))
                                                      .collect(Collectors.toList()));
    }
    
    
    
    @POST
    @Path("/topics/{topic}/events")
    public void submitEvent(@Context UriInfo uriInfo,
                            @PathParam("topic") String topicname,
                            @HeaderParam("Content-Type") String contentType, 
                            InputStream jsonObjectStream,
                            @Suspended AsyncResponse response) throws BadRequestException {
        
        final String topicPath = uriInfo.getBaseUriBuilder().path("topics").path(topicname).toString();
  
        
        if (isCollectionType(contentType)) {
            CompletableFuture<ImmutableList<KafkaMessageId>> sendFuture = CompletableFuture.completedFuture(ImmutableList.of());
            
            for (AvroMessage avroMessage : mapperRepository.toAvroMessages(jsonObjectStream, contentType.replace(".list+json", "+json"))) {
                CompletableFuture<ImmutableList<KafkaMessageId>> future = kafkaProducer.sendAsync(new ProducerRecord<String, byte[]>(topicname, avroMessage.getData()))    
                                                                                       .thenApply(metadata -> ImmutableList.of(new KafkaMessageId(metadata.partition(), metadata.offset())));

                sendFuture = sendFuture.thenCombine(future, (ids1, ids2) -> ImmutableList.<KafkaMessageId>builder().addAll(ids1).addAll(ids2).build());
            }

            sendFuture.thenApply(ids -> topicPath + "/eventcollections/" + KafkaMessageId.toString(ids))
                      .thenApply(uri -> Response.created(URI.create(uri)).build())
                      .whenComplete(ResultConsumer.writeTo(response));
            
            
        } else {
            kafkaProducer.sendAsync(new ProducerRecord<String, byte[]>(topicname, mapperRepository.toAvroMessage(jsonObjectStream, contentType).getData()))    
                         .thenApply(metadata -> new KafkaMessageId(metadata.partition(), metadata.offset()))
                         .thenApply(id -> topicPath + "/events/" + id)
                         .thenApply(uri -> Response.created(URI.create(uri)).build())
                         .whenComplete(ResultConsumer.writeTo(response));;
        }
    }
    

    private boolean isCollectionType(String contentType) {
        return contentType.toLowerCase(Locale.US).endsWith(".list+json");
    }

    
    @GET
    @Path("/topics/{topic}/events/{id}")
    public void readEvent(@PathParam("topic") String topic,
                          @PathParam("id") KafkaMessageId id,
                          @HeaderParam("Accept") @DefaultValue("*/*") MediaType readerMimeType,
                          @Suspended AsyncResponse response) throws IOException {
        
        
        // open kafka source
        ReactiveSource<KafkaMessage<String, byte[]>> reactiveSource = kafkaSourcePrototype.withTopic(topic)
                                                                                          .filter(ImmutableList.of(id))
                                                                                          .open();
        
        // read one message, close source and return result
        reactiveSource.readAsync()
                      .thenApply(kafkaMessage -> mapperRepository.toAvroMessage(kafkaMessage.value(), readerMimeType))
                      .thenApply(avroMessage -> Pair.of(avroMessage.getMimeType(), mapperRepository.toJson(avroMessage)))
                      .thenApply(typeJsonMessagePair -> Response.ok(typeJsonMessagePair.getSecond().toString().getBytes(Charsets.UTF_8))
                                                                .type(typeJsonMessagePair.getFirst())
                                                                .build())
                      .whenComplete((resp, error) -> { reactiveSource.close(); ResultConsumer.writeTo(response).accept(resp, error); });
    }
    
    
    
    @GET
    @Path("/topics/{topic}/eventcollections/{ids}")
    public void readEvents(@PathParam("topic") String topic,
                           @PathParam("ids") String ids,
                           @HeaderParam("Accept") @DefaultValue("*/*") MediaType readerMimeType,
                           @Suspended AsyncResponse response) throws IOException {
        
        
        // TODO make it more functional, async 
        
        try (ReactiveSource<KafkaMessage<String, byte[]>> reactiveSource = kafkaSourcePrototype.withTopic(topic)
                                                                                               .filter(KafkaMessageId.valuesOf(ids))
                                                                                               .open()) {

            
            AtomicReference<MediaType> mimetypeRef = new AtomicReference<>(MediaType.APPLICATION_JSON_TYPE);
            JsonArrayBuilder builder = Json.createArrayBuilder();


            List<KafkaMessageId> identifiers = Lists.newArrayList(KafkaMessageId.valuesOf(ids));
            while (!identifiers.isEmpty()) {
                
                KafkaMessage<String, byte[]> msg = reactiveSource.read(Duration.ofSeconds(3));
                KafkaMessageId id = KafkaMessageId.valueOf(msg.partition(), msg.offset());
                
                if (identifiers.contains(id)) {
                    identifiers.remove(KafkaMessageId.valueOf(msg.partition(), msg.offset()));

                    AvroMessage avroMessage = mapperRepository.toAvroMessage(msg.value(), MediaType.valueOf(readerMimeType.toString().replace(".list+json", "+json")));
                    
                    mimetypeRef.set(MediaType.valueOf(avroMessage.getMimeType().toString().replace("+json", ".list+json")));
                    builder.add(mapperRepository.toJson(avroMessage));
                }
            }
                
            response.resume(Response.ok(builder.build().toString().getBytes(Charsets.UTF_8))
                                    .type(mimetypeRef.get()).build());
        }
    }
    
    

    

    @GET
    @Path("/topics/{topic}/events")
    @Produces("text/event-stream")
    public void readEventsAsStream(@PathParam("topic") String topic,
                                   @HeaderParam("Last-Event-Id") String consumedOffsets,
                                   @Context HttpServletRequest req,
                                   @Context HttpServletResponse resp,
                                   @Suspended AsyncResponse response) {

        resp.setContentType("text/event-stream");
        
        
        // parse filter params
        final Predicate<AvroMessage> filterCondition = FilterCondition.from(ImmutableMap.copyOf(req.getParameterMap()));
        final MediaType readerMimeType = (req.getParameter("q.event.eq") == null) ? MediaType.valueOf("*/*")
                                                                                  : MediaType.valueOf(req.getParameter("q.event.eq")); 

        ImmutableList<MediaType> readerMimeTypes = ImmutableList.of(readerMimeType);
        
        // compose greeting message 
        String prologComment = "stream opened. emitting " + readerMimeType + " event types";
        prologComment = (consumedOffsets == null) ? prologComment : prologComment + " with offset id " + KafkaMessageId.valuesOf(consumedOffsets); 
        prologComment = (filterCondition == FilterCondition.NO_FILTER) ? prologComment : prologComment + " with filter condition " + filterCondition;

        
        // establish reactive response stream 
        final Observable<ServerSentEvent> obs = RxReactiveStreams.toObservable(kafkaSourcePrototype.withTopic(topic).fromOffsets(KafkaMessageId.valuesOf(consumedOffsets)))
                                                                 .map(message -> Pair.of(message.getConsumedOffsets(), mapperRepository.toAvroMessage(message.value(), readerMimeTypes)))
                                                                 .filter(pair -> filterCondition.test(pair.getSecond()))
                                                                 .map(idMsgPair -> mapperRepository.toServerSentEvent(idMsgPair.getFirst(), idMsgPair.getSecond()));
        RxReactiveStreams.toPublisher(obs).subscribe(new ServletSseSubscriber(req, resp, prologComment));
    }
    
   
    
    
    private static final class FilterCondition {

        public static final Predicate<AvroMessage> NO_FILTER = new Condition("", record -> true); 
        
        public static Predicate<AvroMessage> from(ImmutableMap<String, String[]> filterParams) {
            Predicate<AvroMessage> filter = NO_FILTER;
            
            for (Entry<String, String[]> entry : filterParams.entrySet()) {
                if (entry.getKey().startsWith("q.data.")) {
                    for (String value : entry.getValue()) {
                        
                        filter = filter.and(newFilterCondition(entry.getKey(), value));
                    }
                }
            }
            
            return filter;
        }
        
        
        private static final Predicate<AvroMessage> newFilterCondition(String condition, String value) {
            String name = condition.substring("q.data.".length(), condition.lastIndexOf("."));
            
            if (condition.endsWith(".eq")) {
                return new Condition(condition + "=" + value,
                                     record -> read(name, record).map(obj -> obj.toString().equals(value))
                                                                   .orElse(false));
                
            } else if (condition.endsWith(".ne")) {
                return new Condition(condition + "=" + value,
                                     record -> read(name, record).map(obj -> !obj.toString().equals(value))
                                                                   .orElse(false));

            } else if (condition.endsWith(".in")) {
                ImmutableList<String> values = ImmutableList.copyOf(Splitter.on(",").trimResults().splitToList(value));
                return new Condition(condition + "=" + values, 
                                     record -> read(name, record).map(obj -> values.contains(obj.toString()))
                                                                   .orElse(false));

            } else if (condition.endsWith(".gt")) {
                return new Condition(condition + "=" + value,
                                     record -> read(name, record).map(obj -> obj.toString().compareTo(value) > 0)
                                                                   .orElse(false));
                
            } else if (condition.endsWith(".lt")) {
                return new Condition(condition + "=" + value,
                                     record -> read(name, record).map(obj -> obj.toString().compareTo(value) < 0)
                                                                   .orElse(false));

            } else {
                throw new BadRequestException("unsupported filter condition" + condition);
            }
        }
        
        
        private static class Condition implements Predicate<AvroMessage> {
            private final Predicate<AvroMessage> predicate;
            private final String desc;
            
            private Condition(String desc, Predicate<AvroMessage> predicate) {
                this.desc = desc;
                this.predicate = predicate;
            }
            
            @Override
            public boolean test(AvroMessage avroMessage) {
                return predicate.test(avroMessage);
            }
            
            @Override
            public Predicate<AvroMessage> and(Predicate<? super AvroMessage> other) {
                String description = toString();
                description = ((description.length() > 0) && (!description.endsWith("&")) ? "&" : "") + description;
                description = description + other.toString();
                
                return new Condition(description, predicate.and(other));
            }
            
            @Override
            public String toString() {
                return desc;
            }
        }
        
        
        
        private static Optional<Object> read(String dotSeparatedName, AvroMessage avroMessage) {
            
            Optional<Object> result = Optional.empty();
            GenericRecord record = avroMessage.getGenericRecord();
            
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