package net.oneandone.reactive.kafka.rest;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

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
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;

import net.oneandone.reactive.kafka.CompletableKafkaProducer;
import net.oneandone.reactive.rest.container.ResultConsumer;


@Path("/")
public class KafkaResource implements Closeable {
    
    private final AvroSchemaRegistry avroSchemaRegistry = new AvroSchemaRegistry(); 
    private final CompletableKafkaProducer<String, byte[]> kafkaProducer;
    
    
    public KafkaResource(String bootstrapservers) {
        this.kafkaProducer = new CompletableKafkaProducer<>(ImmutableMap.of("bootstrap.servers", bootstrapservers,
                                                                            "key.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class,
                                                                            "value.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class));
    }
    
    @Override
    public void close() throws IOException {
        kafkaProducer.close();
    }
    

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public Object getRoot(@Context UriInfo uriInfo) {
        return ImmutableMap.of("_links", LinksBuilder.newLinksBuilder(uriInfo.getAbsolutePathBuilder().build())
                                                     .withHref("schemas", uriInfo.getAbsolutePathBuilder().path("schemas").build())
                                                     .withHref("topics", uriInfo.getAbsolutePathBuilder().path("topics").build())
                                                     .build());
    }
    
        
    @GET
    @Path("/topics")
    @Produces(MediaType.APPLICATION_JSON)
    public ImmutableList<TopicRepresentation> getTopic(@Context UriInfo uriInfo, @QueryParam("q.topic.name.eq") String topicname) {
        
        return ImmutableList.of(new TopicRepresentation(LinksBuilder.newLinksBuilder(uriInfo.getAbsolutePathBuilder().path(topicname).build())
                                                                    .withHref("events", uriInfo.getAbsolutePathBuilder().path(topicname).path("events").build())
                                                                    .build(),
                                                       topicname));
    }
    
    
    
    @POST
    @Path("/topics/{topic}/events")
    public void consume(@Context UriInfo uriInfo,
                        @PathParam("topic") String topic,
                        @HeaderParam("Content-Type") String contentType, 
                        String jsonObject,
                        @Suspended AsyncResponse response) throws BadRequestException {

        final UriBuilder uriBuilder = uriInfo.getAbsolutePathBuilder();
        
        
        final ImmutableList<byte[]> kafkaMessages = avroSchemaRegistry.getJsonToAvroMapper(contentType)
                                                                      .map(mapper -> mapper.toAvro(jsonObject))
                                                                      .orElseThrow(BadRequestException::new);  
        
                
        sendAsync(topic, kafkaMessages)
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
    public void produceStream(@PathParam("topic") String topic,
                              @HeaderParam("Last-Event-Id") String lastEventId,
                              @Suspended AsyncResponse response) {
        
        // starts reading the stream base on the last event id (if present)
        // To be implemented
    }
    
    
    
    

    @GET
    @Path("/schemas")
    @Produces(MediaType.TEXT_PLAIN)
    public String getRegisteredSchematas() {
        final StringBuilder builder = new StringBuilder();
        
        for (Entry<String, JsonToAvroMapper> entry : avroSchemaRegistry.getRegisteredMapper().entrySet()) {
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
    
    
    
    
    
    
    private static abstract class JsonToAvroMapper {
        
        static final ObjectMapper objectMapper = new ObjectMapper();
        private static final IdlToJson idlToJson = new IdlToJson();
        
        private final MediaType mediaType;

        
        public JsonToAvroMapper(MediaType mediaType) {
            this.mediaType = mediaType;
        }
        

        public MediaType getMediaType() {
            return mediaType;
        }
        
        
        abstract ImmutableList<byte[]> toAvro(String jsonObject);
        
        
        
        protected static String jsonPrettyPrint(String jsonString) {
            try {
                Object json = new ObjectMapper().readValue(jsonString, Object.class);
                return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(json);
            } catch (IOException ioe) {
                return jsonString;
            }
        }
        
        
        public static ImmutableList<JsonToAvroMapper> createMappers(URI schemaURI) throws IOException {
            
            final ImmutableList<JsonNode> jsonSchemas;
            
            // avro json schema? 
            if (schemaURI.getPath().endsWith(".avsc")) {
                jsonSchemas = ImmutableList.of(objectMapper.readTree(Resources.toString(schemaURI.toURL(), Charsets.UTF_8)));
                
            // avro idl?                
            } else if (schemaURI.getPath().endsWith(".avdl")) {
                jsonSchemas = idlToJson.idlToJsonSchemaList(schemaURI);
            
            // unknown!
            } else {
                throw new RuntimeException("unsupported schema file " + schemaURI + " (supported type: .asvc and .avdl)");
            }
            
            
            final List<JsonToAvroMapper> mappers = Lists.newArrayList();
            for (JsonNode jsonSchema : jsonSchemas) {
                
                // entity mapper
                final MediaType entityType = new MediaType("application", "vnd." + Joiner.on(".").join(jsonSchema.get("namespace").asText(), jsonSchema.get("name").asText()) + "+json");
                final JsonToAvroEntityMapper entityMapper = new JsonToAvroEntityMapper(entityType, jsonSchema.toString());
                mappers.add(entityMapper);
                
                // collection mapper
                final MediaType collType = new MediaType("application", "vnd." + Joiner.on(".").join(jsonSchema.get("namespace").asText(), jsonSchema.get("name").asText()) + ".list+json");
                final JsonToAvroCollectionMapper collectionMapper = new JsonToAvroCollectionMapper(collType, entityMapper);
                mappers.add(collectionMapper);
            }

            return ImmutableList.copyOf(mappers);
        }
        

        

        private static final class IdlToJson {
            
            public ImmutableList<JsonNode> idlToJsonSchemaList(URI idlUrl) {
                
                InputStream is = null;
                Idl parser = null;
                try {
                    is = idlUrl.toURL().openStream();
                    parser = new Idl(is);
                    
                    final String idl = parser.CompilationUnit().toString(true);
                    final JsonNode idlNode = objectMapper.readTree(idl);
          
                    final List<JsonNode> result = Lists.newArrayList();
                    for (JsonNode typeNode : idlNode.get("types")) {
                        ((ObjectNode) typeNode).put("namespace", idlNode.get("namespace"));
                        result.add(typeNode);
                    }
                    
                    return ImmutableList.copyOf(result);
                    
                } catch (ParseException | IOException pe) {
                    throw new RuntimeException(pe);
                    
                } finally {
                    Closeables.closeQuietly(is);
                    try {
                        Closeables.close(parser, true);
                    } catch (IOException ignore) { }
                }
            }
        }
    }
        
    
    

    private static final class JsonToAvroCollectionMapper extends JsonToAvroMapper {
        
        private final JsonToAvroEntityMapper entityMapper;
    
        
        public JsonToAvroCollectionMapper(MediaType mediaType, JsonToAvroEntityMapper entityMapper) {
            super(mediaType);
            this.entityMapper = entityMapper;
        }
        
        
        @Override
        ImmutableList<byte[]> toAvro(String jsonObject) {
            
            try {
                final List<byte[]> avroMesssages = Lists.newArrayList();
                for (JsonNode jsonMessage : objectMapper.readTree(jsonObject)) {
                    avroMesssages.addAll(entityMapper.toAvro(jsonMessage.toString()));
                }
            
                return ImmutableList.copyOf(avroMesssages); 
                
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
        
        @Override
        public String toString() {
            return jsonPrettyPrint("[\r\n" + entityMapper.toString() + "\r\n]");
        }
    }

    
    
    private static final class JsonToAvroEntityMapper extends JsonToAvroMapper {
    
        private final String schemaAsString;
        private final Schema schema;
        private final GenericDatumWriter<Object> writer;
        
        

        private JsonToAvroEntityMapper(MediaType mediaType, String schemaAsString) {
            super(mediaType);
            this.schemaAsString = schemaAsString;
            this.schema = new Schema.Parser().parse(schemaAsString);
            this.writer = new GenericDatumWriter<Object>(schema);
        }
        
        
        
        @Override
        public ImmutableList<byte[]> toAvro(String jsonObject) {
            try {
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                
                final Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new DataInputStream(new ByteArrayInputStream(jsonObject.getBytes(Charsets.UTF_8))));
                final Object datum = new GenericDatumReader<Object>(schema).read(null, decoder);
                
                final Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                writer.write(datum, encoder);
                encoder.flush();
               
                return ImmutableList.of(outputStream.toByteArray());
                
            } catch (IOException ioe) {
                throw new BadRequestException("schema conflict " + ioe);
            }
        }
        
        @Override
        public String toString() {
            return jsonPrettyPrint(schemaAsString);
        }
    }
    
    
    
    
    

    // TODO replace this: managing the local schema registry should be replaced by a data-replicator based approach
    // * the schema definition files will by loaded over URI by the data replicator, periodically
    // * the mime type is extracted from the filename by using naming conventions  

    private static final class AvroSchemaRegistry {
        private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaRegistry.class);
        
        private volatile ImmutableMap<String, JsonToAvroMapper> jsonToAvroWriters = ImmutableMap.of();

        
        
        public AvroSchemaRegistry() {
            final File dir = new File("src" + File.separator + "test" + File.separator + "resources" + File.separator + "schemas");
            reloadSchemadefintions(ImmutableList.copyOf(ImmutableList.copyOf(dir.listFiles()).stream()
                                                                                             .map(file -> file.toURI())
                                                                                             .collect(Collectors.toList())));
        }
            
        
        
        public void reloadSchemadefintions(ImmutableList<URI> schemafileUris) {
            
            
            final Map<String, JsonToAvroMapper> newJsonToAvroWriters = Maps.newHashMap();
            
            for (URI fileUri : schemafileUris) {
                try {
                    newJsonToAvroWriters.putAll(JsonToAvroEntityMapper.createMappers(fileUri)
                                                                .stream()
                                                                .collect(Collectors.toMap(mapper -> mapper.getMediaType().toString(),
                                                                                          mapper -> mapper)));
                    
                } catch (IOException ioe) {
                    LOG.warn("error loading avro schema " + fileUri, ioe);
                }
            }
            
            jsonToAvroWriters = ImmutableMap.copyOf(newJsonToAvroWriters);
        }
        
     
        public ImmutableMap<String, JsonToAvroMapper> getRegisteredMapper() {
            return jsonToAvroWriters;
        }
        
        public Optional<JsonToAvroMapper> getJsonToAvroMapper(String mimeType) {
            return Optional.ofNullable(jsonToAvroWriters.get(mimeType));
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
}