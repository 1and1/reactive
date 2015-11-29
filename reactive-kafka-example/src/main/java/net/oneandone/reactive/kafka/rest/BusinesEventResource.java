package net.oneandone.reactive.kafka.rest;

import java.io.InputStream;

import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.concurrent.CompletableFuture;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.BadRequestException;
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
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;

import net.oneandone.avro.json.JsonAvroMapperRegistry;
import net.oneandone.avro.json.SchemaException;
import net.oneandone.avro.json.JsonAvroMapper;
import net.oneandone.reactive.kafka.CompletableKafkaProducer;
import net.oneandone.reactive.kafka.rest.KafkaSource.ConsumedOffsets;
import net.oneandone.reactive.pipe.Pipes;
import net.oneandone.reactive.rest.container.ResultConsumer;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.servlet.ServletSseSubscriber;
import net.oneandone.reactive.utils.Pair;





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
        return ImmutableMap.of("_links", LinksBuilder.newLinksBuilder(uriInfo.getAbsolutePathBuilder().build())
                                                     .withHref("topics", uriInfo.getAbsolutePathBuilder().path("topics").build())
                                                     .build());
    }
    
        
    @GET
    @Path("/topics")
    @Produces("application/vnd.ui.mam.eventservice.topic.list+json")
    public TopicsRepresentation getTopics(@Context UriInfo uriInfo, @QueryParam("q.topic.name.eq") String topicname) {
        return new TopicsRepresentation(LinksBuilder.newLinksBuilder(uriInfo.getAbsolutePathBuilder().build()).build(),
                                        ImmutableList.of(getTopic(uriInfo, topicname))); 
    }
    
    
    
    @GET
    @Path("/topics/{topicname}")
    @Produces("application/vnd.ui.mam.eventservice.topic+json")
    public TopicRepresentation getTopic(@Context UriInfo uriInfo, @PathParam("topicname") String topicname) {
        return new TopicRepresentation(LinksBuilder.newLinksBuilder(uriInfo.getAbsolutePathBuilder().path(topicname).build())
                                                   .withHref("events", uriInfo.getAbsolutePathBuilder().path(topicname).path("events").build())
                                                   .withHref("schemas", uriInfo.getAbsolutePathBuilder().path(topicname).path("schemas").build())
                                                   .build(),
                                       topicname);
    }
        
    
    @POST
    @Path("/topics/{topic}/events")
    public void consume(@Context UriInfo uriInfo,
                        @PathParam("topic") String topic,
                        @HeaderParam("Content-Type") String contentType, 
                        InputStream jsonObjectStream,
                        @Suspended AsyncResponse response) throws BadRequestException {

        final UriBuilder uriBuilder = uriInfo.getAbsolutePathBuilder();
         
        final ImmutableList<byte[]> avroMessages = jsonAvroMapperRegistry.getJsonToAvroMapper(contentType)
                                                                         .map(mapper -> AvroMessageSerializer.serialize(mapper.getSchema(), mapper.toAvroRecord(jsonObjectStream)))
                                                                         .orElseThrow(BadRequestException::new);  
        
        sendAsync(topic, avroMessages)
                .thenApply(ids -> Response.created(contentType.toLowerCase(Locale.US).endsWith(".list+json") ? uriBuilder.path("eventviews").path(new ViewId(ids).serialize()).build() 
                                                                                                             : uriBuilder.path(ids.get(0).serialize()).build()).build())
                .whenComplete(ResultConsumer.writeTo(response));
    }
    
        
    private CompletableFuture<ImmutableList<KafkaMessageId>> sendAsync(String topic, ImmutableList<byte[]> kafkaMessages) {
        return sendAsync(topic, kafkaMessages, ImmutableList.of());
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
                        @Suspended AsyncResponse response) {
        
        // To be implemented
        response.resume("OK (" + id + ")");
    }
    
    
    @GET
    @Path("/topics/{topic}/eventviews/{viewId}")
    public void produceView(@PathParam("topic") String topic,
                            @PathParam("viewId") ViewId id,
                            @Suspended AsyncResponse response) {
        
        // To be implemented
        response.resume("OK (" + id + ")");
    }
    
    

    @GET
    @Path("/topics/{topic}/events")
    @Produces("text/event-stream")
    public void produceReactiveStream(@PathParam("topic") String topic,
                                      @HeaderParam("Last-Event-Id") ConsumedOffsets lastEventId,
                                      @QueryParam("q.event.eq") String acceptedEventtype, 
                                      @Context HttpServletRequest req,
                                      @Context HttpServletResponse resp,
                                      @Suspended AsyncResponse response) {

        resp.setContentType("text/event-stream");
        
        final Schema readerSchema = (acceptedEventtype == null) ? null
                                                                : jsonAvroMapperRegistry.getJsonToAvroMapper(acceptedEventtype)
                                                                                        .orElseThrow(BadRequestException::new)
                                                                                        .getSchema();

        
        // compose greeting message 
        final String prologComment = (acceptedEventtype == null) ? "stream opened. emitting all event types"
                                                                 : "stream opened. emitting " + acceptedEventtype + " event types only";
        
        // configure kafka source
        KafkaSource<String, byte[]> kafkaSource = kafkaSourcePrototype.withTopic(topic); 
        kafkaSource = (lastEventId == null) ? kafkaSource : kafkaSource.withConsumedOffsets(lastEventId); 
        
        // and open a reactive stream
        Pipes.from(kafkaSource)      
             .map(message -> Pair.of(message.getConsumedOffsets(), AvroMessageSerializer.deserialize(message.getRecord().value(), jsonAvroMapperRegistry, readerSchema)))
             .map(idMsgPair -> toServerSentEvent(idMsgPair.getFirst(), idMsgPair.getSecond()))
             .to(new ServletSseSubscriber(req, resp, prologComment));  
    }

   
    private ServerSentEvent toServerSentEvent(ConsumedOffsets consumedOffsets, GenericRecord avroMessage) {
        JsonAvroMapper mapper = jsonAvroMapperRegistry.getJsonToAvroMapper(avroMessage.getSchema())
                                                      .orElseThrow(SchemaException::new);
        
        return ServerSentEvent.newEvent().id(consumedOffsets.toString())
                                         .event(mapper.getMimeType())
                                         .data(mapper.toJson(avroMessage).toString());
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
    
    
    
    
    public static final class KafkaMessageId {
        private final static String SEPARATOR = "-";  
        private final static BaseEncoding encoding = BaseEncoding.base64Url(); 
        
        private final int partition;
        private final long offset;
        
        public KafkaMessageId(int partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }
        
        
        public static KafkaMessageId valueOf(String txt) {
            final String decoded = new String(encoding.decode(txt), Charsets.UTF_8);
            final int idx = decoded.indexOf(SEPARATOR);
            
            return new KafkaMessageId(Integer.parseInt(decoded.substring(0, idx)),
                                      Long.parseLong(decoded.substring(idx + SEPARATOR.length(), decoded.length())));
        }
        
        public int getPartition() {
            return partition;
        }

        public long getOffset() {
            return offset;
        }

        @Override
        public String toString() {
            return "partition=" + partition + " offset=" + offset;
        }
        
        public String serialize() {
            return encoding.encode((partition + SEPARATOR + offset).getBytes(Charsets.UTF_8));
        }
    }
    
    
    
    
    public static final class ViewId {
        
        private final ImmutableList<KafkaMessageId> ids;
        
        
        public ViewId(ImmutableList<KafkaMessageId> ids) {
            this.ids = ids;
        }
        
        public ImmutableList<KafkaMessageId> getIds() {
            return ids;
        }
        
        public static ViewId valueOf(String txt) {
            return new ViewId(ImmutableList.copyOf(Splitter.on("+").splitToList(txt).stream().map(id -> KafkaMessageId.valueOf(id)).collect(Collectors.toList())));
        }
        
        @Override
        public String toString() {
            return Joiner.on(", ").join(ids);
        }
        
        public String serialize() {
            return Joiner.on("+").join(ids.stream().map(kafkaId -> kafkaId.serialize()).collect(Collectors.toList()));
        }
    }
    
    
    
    
    
    private static final class LinksBuilder {
        
        private final ImmutableMap<String, Object> links; 
        
        private LinksBuilder(ImmutableMap<String, Object> links) {
            this.links = links;
        }

        public static LinksBuilder newLinksBuilder(URI selfHref) {
            return new LinksBuilder(ImmutableMap.of()).withHref("self", selfHref);
        }
        
        public LinksBuilder withHref(String name, URI href) {
            return new LinksBuilder(ImmutableMap.<String, Object>builder()
                                                .putAll(links)
                                                .put(name, ImmutableMap.of("href", href.toString()))
                                                .build());
        }
        
        
        public ImmutableMap<String, Object> build() {
            return links;
        }
    }
    
    
    public static final class TopicRepresentation {
        
        public ImmutableMap<String, Object> _links; 
        public String name;
        
        public TopicRepresentation() {  }
        
        public TopicRepresentation(ImmutableMap<String, Object> _links, String name) {
            this._links = _links;
            this.name = name;
        }
    }
    
    
    public static final class TopicsRepresentation {
        
        public ImmutableMap<String, Object> _links; 
        public List<TopicRepresentation> _elements;
        
        public TopicsRepresentation() {  }
        
        public TopicsRepresentation(ImmutableMap<String, Object> _links, List<TopicRepresentation> _elements) {
            this._links = _links;
            this._elements = _elements;
        }
    }
}