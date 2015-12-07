package net.oneandone.avro.json;


import java.io.ByteArrayInputStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.Optional;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.ws.rs.core.MediaType;

import org.apache.avro.Schema;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;

import net.oneandone.reactive.kafka.KafkaMessageIdList;
import net.oneandone.reactive.sse.ServerSentEvent;





public class AvroMessageMapperRepository {
    
    private static final Logger LOG = LoggerFactory.getLogger(AvroMessageMapperRepository.class);
    private static final IdlToJson idlToJson = new IdlToJson();
    
    private volatile ImmutableMap<String, AvroMessageMapper> mapper = ImmutableMap.of();
    private volatile ImmutableMap<String, String> schemaNameIndex = ImmutableMap.of();
    private volatile ImmutableMap<String, Schema> schemaIndex = ImmutableMap.of();

    
    
    public AvroMessageMapperRepository(File schemaDir) {
        reloadSchemadefintions(ImmutableList.copyOf(ImmutableList.copyOf(schemaDir.listFiles()).stream()
                                                                                               .map(file -> file.toURI())
                                                                                               .collect(Collectors.toList())));
    } 
        
    
    
    public void reloadSchemadefintions(ImmutableList<URI> schemafileUris) {
        
        List<AvroMessageMapper> mappers = Lists.newArrayList();
        for (URI fileUri : schemafileUris) {
            try {
                mappers.addAll(createMappers(fileUri));
            } catch (IOException ioe) {
                LOG.warn("error loading avro schema " + fileUri, ioe);
            }
        }

        this.mapper = ImmutableMap.copyOf(mappers.stream()
                                                 .collect(Collectors.toMap(AvroMessageMapper::getMimeType, (AvroMessageMapper m) -> m)));
        
        schemaNameIndex = ImmutableMap.copyOf(mapper.values()
                                                    .stream()
                                                    .collect(Collectors.toMap(m -> m.getSchema().getNamespace() + "." + m.getSchema().getName(), 
                                                                              m -> m.getMimeType())));
        schemaIndex = ImmutableMap.copyOf(mapper.values()
                                                .stream()
                                                .collect(Collectors.toMap(m -> m.getSchema().getNamespace() + "." + m.getSchema().getName(), 
                                                                          m -> m.getSchema())));
    }
    


    public ImmutableMap<String, String> getRegisteredSchemas() {
        Map<String, String> schemas = Maps.newHashMap();
        
        for (Entry<String, AvroMessageMapper> entry : mapper.entrySet()) {
            schemas.put(entry.getKey(), entry.getValue().toString());
            schemas.put(entry.getKey().replace("+json", ".list+json"), "[" + entry.getValue().toString() + "\r\n]");
        }
        
        return ImmutableMap.copyOf(schemas);
    }
    

    
    public JsonObject toJson(AvroMessage avroMessage) {
        return getJsonToAvroMapper(avroMessage.getSchema()).map(mapper -> mapper.toJson(avroMessage))
                                                           .orElseThrow(SchemaException::new);
    }

    
    

    public ImmutableList<AvroMessage> toAvroMessages(InputStream jsonObjectStream, String mimeType) {
        return getJsonToAvroMapper(mimeType).map(mapper -> mapper.toAvroMessages(Json.createParser(jsonObjectStream)))
                                            .orElseThrow(SchemaException::new);
    }
    
    
    

    public AvroMessage toAvroMessage(InputStream jsonObjectStream, String mimeType) {
        return getJsonToAvroMapper(mimeType).map(mapper -> mapper.toAvroMessage(Json.createParser(jsonObjectStream)))
                                            .orElseThrow(SchemaException::new);
    }
    
        


    public AvroMessage toAvroMessage(byte[] serializedAvroMessage, MediaType readerMimeType) throws SchemaException {
        return toAvroMessage(serializedAvroMessage, ImmutableList.of(readerMimeType));
    }
    

    
    public AvroMessage toAvroMessage(byte[] serializedAvroMessage, ImmutableList<MediaType> readerMimeTypes) throws SchemaException {
        
        ImmutableList<Schema> readerSchemas = ImmutableList.copyOf(readerMimeTypes.stream()
                                                                                  .map(mimeType -> getJsonToAvroMapper(mimeType.toString()))
                                                                                  .filter(optionalMapper -> optionalMapper.isPresent())
                                                                                  .map(mapper -> mapper.get().getSchema())
                                                                                  .collect(Collectors.toList()));
        
        return AvroMessage.from(serializedAvroMessage, schemaIndex, readerSchemas);
    }
    

    public ServerSentEvent toServerSentEvent(KafkaMessageIdList consumedOffsets, AvroMessage avroMessage) {
        return getJsonToAvroMapper(avroMessage.getSchema()).map(schema -> ServerSentEvent.newEvent()
                                                                                         .id(consumedOffsets.toString())
                                                                                         .event(avroMessage.getMimeType().toString())
                                                                                         .data(toJson(avroMessage).toString()))
                                                           .orElseThrow(SchemaException::new);
    }
    
    
    Schema getSchema(String namespace, String name) {
        return getJsonToAvroMapper(namespace, name).orElseThrow(SchemaException::new)
                                                   .getSchema();
    }
    
    
    private Optional<AvroMessageMapper> getJsonToAvroMapper(Schema schema) {
        return getJsonToAvroMapper(schema.getNamespace(),  schema.getName());
    }
    

    private Optional<AvroMessageMapper> getJsonToAvroMapper(String namespace, String name) {
        return  Optional.ofNullable(schemaNameIndex.get(namespace + "." + name))
                        .map(mimeType -> mapper.get(mimeType));
    }

    
    private Optional<AvroMessageMapper> getJsonToAvroMapper(String mimeType) {
        return Optional.ofNullable(mapper.get(mimeType));
    }
    
    
    
    private ImmutableList<AvroMessageMapper> createMappers(URI schemaURI) throws IOException {
        
        final ImmutableList<JsonObject> jsonSchemas;
        // avro json schema? 
        if (schemaURI.getPath().endsWith(".avsc")) {
            try (InputStream is = schemaURI.toURL().openStream()) {
                jsonSchemas = ImmutableList.of(Json.createReader(is).readObject());
            }
            
        // avro idl?                
        } else if (schemaURI.getPath().endsWith(".avdl")) {
            jsonSchemas = idlToJson.idlToJsonSchemaList(schemaURI);
        
        // unknown!
        } else {
            LOG.info("unsupported schema file " + schemaURI + " found (supported type: .asvc and .avdl)");
            return ImmutableList.of();
        }
        

        List<AvroMessageMapper> mappers = Lists.newArrayList();
        for (JsonObject jsonSchema : jsonSchemas) {
            AvroMessageMapper entityMapper = AvroMessageMapper.createrMapper(jsonSchema);
            mappers.add(entityMapper);
       //     mappers.add(new JsonAvroCollectionMapper(entityMapper));
        }

        return ImmutableList.copyOf(mappers);
    }
    

    
    
    
    private static final class IdlToJson {
        
        public ImmutableList<JsonObject> idlToJsonSchemaList(URI idlUrl) {
            InputStream is = null;
            Idl parser = null;
            try {
                is = idlUrl.toURL().openStream();
                parser = new Idl(is);
                
                final String idl = parser.CompilationUnit().toString(true);
                final JsonReader reader = Json.createReader(new ByteArrayInputStream(idl.getBytes(Charsets.UTF_8)));
                
                
                List<JsonObject> jsonSchemas = Lists.newArrayList();
                JsonObject idlJson =  reader.readObject();
                String namespace = idlJson.getString("namespace");
                
                for (JsonValue recordJson : idlJson.getJsonArray("types")) {
                    JsonObjectBuilder builder = Json.createObjectBuilder();
                
                    if (!((JsonObject) recordJson).containsKey("namespace")) {
                        builder.add("namespace", namespace);
                    }
                    
                    for (Entry<String, JsonValue> nameValuePair : ((JsonObject) recordJson).entrySet()) {
                        builder.add(nameValuePair.getKey(), nameValuePair.getValue());
                    }
                    
                    jsonSchemas.add(builder.build());
                }
                
                return ImmutableList.copyOf(jsonSchemas);
                
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

