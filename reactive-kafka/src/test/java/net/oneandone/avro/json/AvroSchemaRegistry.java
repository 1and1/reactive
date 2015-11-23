package net.oneandone.avro.json;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Optional;
import java.util.function.BiConsumer;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;

import org.apache.avro.generic.GenericRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;





public class AvroSchemaRegistry {
    
    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaRegistry.class);
    private static final IdlToJson idlToJson = new IdlToJson();
    
    private volatile ImmutableMap<String, JsonAvroMapper> jsonToAvroWriters = ImmutableMap.of();

    
    
    public AvroSchemaRegistry() {
        final File dir = new File("src" + File.separator + "test" + File.separator + "resources" + File.separator + "schemas");
        reloadSchemadefintions(ImmutableList.copyOf(ImmutableList.copyOf(dir.listFiles()).stream()
                                                                                         .map(file -> file.toURI())
                                                                                         .collect(Collectors.toList())));
    }
        
    
    
    public void reloadSchemadefintions(ImmutableList<URI> schemafileUris) {
        
        final Map<String, JsonAvroMapper> newJsonToAvroWriters = Maps.newHashMap();
        
        for (URI fileUri : schemafileUris) {
            try {
                newJsonToAvroWriters.putAll(createMappers(fileUri));
            } catch (IOException ioe) {
                LOG.warn("error loading avro schema " + fileUri, ioe);
            }
        }
        
        jsonToAvroWriters = ImmutableMap.copyOf(newJsonToAvroWriters);
    }
    
 
    public ImmutableMap<String, JsonAvroMapper> getRegisteredMapper() {
        return jsonToAvroWriters;
    }
    
    public Optional<JsonAvroMapper> getJsonToAvroMapper(String mimeType) {
        return Optional.ofNullable(jsonToAvroWriters.get(mimeType));
    }
    

    
    private ImmutableMap<String, JsonAvroMapper> createMappers(URI schemaURI) throws IOException {
        
        final ImmutableList<JsonObject> jsonSchemas;
        
        // avro json schema? 
        if (schemaURI.getPath().endsWith(".avsc")) {
            try (InputStream is = schemaURI.toURL().openStream()) {
                final JsonReader reader = Json.createReader(is);
                jsonSchemas = ImmutableList.of(reader.readObject());
            }
            
        // avro idl?                
        } else if (schemaURI.getPath().endsWith(".avdl")) {
            // jsonSchemas = idlToJson.idlToJsonSchemaList(schemaURI);
            return ImmutableMap.of();
        
        // unknown!
        } else {
            LOG.info("unsupported schema file " + schemaURI + " found (supported type: .asvc and .avdl)");
            return ImmutableMap.of();
        }
        

        Map<String, JsonAvroMapper> mappers = Maps.newHashMap();
        for (JsonObject jsonSchema : jsonSchemas) {
           
            final String type = "application/vnd." + Joiner.on(".").join( ((JsonString) jsonSchema.get("namespace")).getString(),  
                                                                          ((JsonString) jsonSchema.get("name")).getString());
            
            // entity mapper
            final JsonAvroEntityMapper entityMapper = JsonAvroEntityMapper.createrMapper(jsonSchema);
            mappers.put(type + "+json", JsonAvroEntityMapper.createrMapper(jsonSchema));
            
            // collection mapper
            final JsonAvroCollectionMapper collectionMapper = new JsonAvroCollectionMapper(entityMapper);
            mappers.put(type + ".list+json", collectionMapper);
        }

        return ImmutableMap.copyOf(mappers);
    }
    

    private static final class JsonAvroCollectionMapper implements JsonAvroMapper {
        
        private final JsonAvroEntityMapper entityMapper;
    
        
        public JsonAvroCollectionMapper(JsonAvroEntityMapper entityMapper) {
            this.entityMapper = entityMapper;
        }
        
        
        @Override
        public ImmutableList<byte[]> toAvroBinaryRecord(JsonParser jsonParser) {
            return ImmutableList.copyOf(toAvroRecord(jsonParser).stream()
                                                                .map(record -> JsonAvroEntityMapper.serialize(record, entityMapper.getSchema()))
                                                                .collect(Collectors.toList()));
        }
        
        
        @Override
        public ImmutableList<GenericRecord> toAvroRecord(JsonParser jsonParser) {

            // check initial state
            if (jsonParser.next() != Event.START_ARRAY) {
                throw new IllegalStateException("START_ARRAY event excepted");
            }

            
            final List<GenericRecord> avroRecords = Lists.newArrayList();
            
            
            while (jsonParser.hasNext()) {
                switch (jsonParser.next()) {

                
                case END_ARRAY:
                    return ImmutableList.copyOf(avroRecords);

                case START_OBJECT:
                    avroRecords.add(entityMapper.toSingleAvroRecord(jsonParser));
                    break;
                    
                default:
                    System.out.println("");
                }
            }

            
            throw new IllegalStateException("END_ARRAY event is missing");
            
        }

        
        @Override
        public String toString() {
            return "[" + entityMapper.toString() + "\r\n]";
        }
    }
    

    
    
    private static final class IdlToJson {
        
        public ImmutableList<JsonObject> idlToJsonSchemaList(URI idlUrl) {
/*            
            InputStream is = null;
            Idl parser = null;
            try {
                is = idlUrl.toURL().openStream();
                parser = new Idl(is);
                
                final String idl = parser.CompilationUnit().toString(true);
                final JsonReader reader = Json.createReader(new ByteArrayInputStream(idl.getBytes(Charsets.UTF_8)));
                final JsonObject idlJsonSchema = reader.readObject();
                
                final List<JsonNode> result = Lists.newArrayList();
                for (JsonValue typeJson : idlJsonSchema.getJsonArray("types")) {
                    JsonValue namespaceJson = ((JsonObject) typeJson).get("namespace");
                    
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
        */
            
            return ImmutableList.of();
        }            
    }

 
    
}

