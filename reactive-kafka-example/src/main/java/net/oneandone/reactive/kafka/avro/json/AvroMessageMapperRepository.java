/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.reactive.kafka.avro.json;



import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

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
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

import net.oneandone.commons.incubator.datareplicator.DataReplicator;
import net.oneandone.commons.incubator.datareplicator.ReplicationJob;
import net.oneandone.reactive.kafka.KafkaMessageIdList;
import net.oneandone.reactive.sse.ServerSentEvent;





public class AvroMessageMapperRepository implements Closeable {
    
    private static final Logger LOG = LoggerFactory.getLogger(AvroMessageMapperRepository.class);
    private static final IdlToJson idlToJson = new IdlToJson();

    private final ReplicationJob replicationJob; 
    private volatile ImmutableMap<String, AvroMessageMapper> mapper = ImmutableMap.of();
    private volatile ImmutableMap<String, String> schemaNameIndex = ImmutableMap.of();
    private volatile ImmutableMap<String, Schema> schemaIndex = ImmutableMap.of();

    

    
    
    public AvroMessageMapperRepository(URI uri) {
        this.replicationJob = new DataReplicator(uri).withRefreshPeriod(Duration.ofMinutes(1))
                                                     .open(this::onRefresh);
    }
    
    
    @Override
    public void close() throws IOException {
        replicationJob.close();
    }

    
    public void onRefresh(byte[] binary) {
        
        List<AvroMessageMapper> mappers = Lists.newArrayList();
        
        for (Entry<String, InputStream> entry : Zip.unpack(binary).entrySet()) {
            
            try {
                mappers.addAll(createMappers(entry.getKey(), entry.getValue()));
            } catch (IOException ioe) {
                LOG.warn("error loading avro schema " + entry.getKey(), ioe);
            }
        }
        

        this.mapper = ImmutableMap.copyOf(mappers.stream()
                                                 .collect(Collectors.toMap(AvroMessageMapper::getMimeType, (AvroMessageMapper m) -> m)));
        
        schemaNameIndex = ImmutableMap.copyOf(mapper.values()
                                                    .stream()
                                                    .collect(Collectors.toMap(m -> m.getSchema().getFullName(), 
                                                                              m -> m.getMimeType())));
        schemaIndex = ImmutableMap.copyOf(mapper.values()
                                                .stream()
                                                .collect(Collectors.toMap(m -> m.getSchema().getFullName(), 
                                                                          m -> m.getSchema())));
    }    
    
    



    public ImmutableMap<String, String> getRegisteredSchemasAsText() {
        Map<String, String> schemas = Maps.newHashMap();
        
        for (Entry<String, AvroMessageMapper> entry : mapper.entrySet()) {
            schemas.put(entry.getKey(), entry.getValue().toString());
            schemas.put(entry.getKey().replace("+json", ".list+json"), "[" + entry.getValue().toString() + "\r\n]");
        }
        
        return ImmutableMap.copyOf(schemas);
    }
    
    

    public  ImmutableMap<String, Schema> getRegisteredSchemas() {
        Map<String, Schema> schemas = Maps.newHashMap();
        
        for (Entry<String, AvroMessageMapper> entry : mapper.entrySet()) {
            schemas.put(entry.getKey(), entry.getValue().getSchema());
        }
        
        return ImmutableMap.copyOf(schemas);
    }
    

    public Optional<String> getRegisteredSchema(MediaType mimetype) {
        
        for (Entry<String, String> entry : getRegisteredSchemasAsText().entrySet()) {
            if (entry.getKey().equals(mimetype.toString())) {
                return Optional.of(entry.getValue().toString());
            }
        }
        
        return Optional.empty();
    }
    

    
    public JsonObject toJson(AvroMessage avroMessage) {
        return getJsonToAvroMapper(avroMessage.getSchema()).map(mapper -> mapper.toJson(avroMessage))
                                                           .orElseThrow(() -> new SchemaException("unsupported type", avroMessage.getMimeType().toString()));
    }

    
    

    public ImmutableList<AvroMessage> toAvroMessages(InputStream jsonObjectStream, String mimeType) {
        return getJsonToAvroMapper(mimeType).map(mapper -> mapper.toAvroMessages(Json.createParser(jsonObjectStream)))
                                            .orElseThrow(() -> new SchemaException("unsupported type", mimeType));
    }
    
    
    

    public AvroMessage toAvroMessage(InputStream jsonObjectStream, String mimeType) {
        return getJsonToAvroMapper(mimeType).map(mapper -> mapper.toAvroMessage(Json.createParser(jsonObjectStream)))
                                            .orElseThrow(() -> new SchemaException("unsupported type", mimeType));
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
        
        AvroMessage avroMessage = AvroMessage.from(serializedAvroMessage, schemaIndex, readerSchemas);

        for (MediaType readerMimeType : readerMimeTypes) {
            if (avroMessage.getMimeType().isCompatible(readerMimeType)) {
                return avroMessage;
            }
        }

        throw new SchemaException("avro messaqge type " + avroMessage.getMimeType() + " does not match with a requested type " + readerMimeTypes);
    }
    

    public ServerSentEvent toServerSentEvent(KafkaMessageIdList consumedOffsets, AvroMessage avroMessage) {
        return getJsonToAvroMapper(avroMessage.getSchema()).map(schema -> ServerSentEvent.newEvent()
                                                                                         .id(consumedOffsets.toString())
                                                                                         .data(avroMessage.getMimeType().toString() + "\n" +
                                                                                               toJson(avroMessage).toString()))
                                                           .orElseThrow(() -> new SchemaException("unsupported type", avroMessage.getMimeType().toString()));
    }
    
    
    Schema getSchema(String namespace, String name) {
        return getJsonToAvroMapper(namespace, name).orElseThrow(() -> new SchemaException("unsupported type", namespace + "." + name))
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
    
    
    
    private ImmutableList<AvroMessageMapper> createMappers(String filename, InputStream is) throws IOException {
        
        
        final ImmutableList<JsonObject> jsonSchemas;
        
        // avro json schema? 
        if (filename.endsWith(".avsc")) {
            jsonSchemas = ImmutableList.of(Json.createReader(is).readObject());
            
        // avro idl?                
        } else if (filename.endsWith(".avdl")) {
            jsonSchemas = idlToJson.idlToJsonSchemaList(is);
        
        // unknown!
        } else {
            LOG.info("unsupported schema file " + filename + " found (supported type: .asvc and .avdl)");
            return ImmutableList.of();
        }
        
        

        List<AvroMessageMapper> mappers = Lists.newArrayList();
        for (JsonObject jsonSchema : jsonSchemas) {
            AvroMessageMapper entityMapper = AvroMessageMapper.createrMapper(jsonSchema);
            mappers.add(entityMapper);
        }

        return ImmutableList.copyOf(mappers);
    }
    

    
    
    
    private static final class IdlToJson {
        
        public ImmutableList<JsonObject> idlToJsonSchemaList(InputStream data) {
            
            InputStream is = null;
            Idl parser = null;
            try {
                parser = new Idl(data);
                
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
                
            } catch (ParseException pe) {
                throw new RuntimeException(pe);
                
            } finally {
                Closeables.closeQuietly(is);
                try {
                    Closeables.close(parser, true);
                } catch (IOException ignore) { }
            }
        }            
    }
    
    
    private static class Zip {

        private Zip() { }
        
        
        public static ImmutableMap<String, InputStream> unpack(byte[] binary) {
            
            Map<String, InputStream> result = Maps.newHashMap();

            try {
                ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(binary));
                
                ZipEntry ze = null;
                while ((ze = zin.getNextEntry()) != null) {
                    result.put(ze.getName(), new ByteArrayInputStream(ByteStreams.toByteArray(zin)));
                    zin.closeEntry();
                }
                zin.close();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            
            return ImmutableMap.copyOf(result);
        }
    }
}
