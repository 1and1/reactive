package net.oneandone.avro.json.schemaregistry;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.Optional;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

import net.oneandone.avro.json.JsonAvroCollectionMapper;
import net.oneandone.avro.json.JsonAvroEntityMapper;
import net.oneandone.avro.json.JsonAvroMapper;
import net.oneandone.avro.json.SchemaName;





public class JasonAvroMapperRegistry {
    
    private static final Logger LOG = LoggerFactory.getLogger(JasonAvroMapperRegistry.class);
    private static final IdlToJson idlToJson = new IdlToJson();
    
    private volatile ImmutableMap<String, JsonAvroMapper> jsonAvroMapperByMimeType = ImmutableMap.of();
    private volatile ImmutableMap<SchemaName, JsonAvroMapper> jsonAvroMapperBySchemaName = ImmutableMap.of();

    
    
    public JasonAvroMapperRegistry(File schemaDir) {
        reloadSchemadefintions(ImmutableList.copyOf(ImmutableList.copyOf(schemaDir.listFiles()).stream()
                                                                                               .map(file -> file.toURI())
                                                                                               .collect(Collectors.toList())));
    } 
        
    
    
    public void reloadSchemadefintions(ImmutableList<URI> schemafileUris) {
        
        List<JsonAvroMapper> mappers = Lists.newArrayList();
        for (URI fileUri : schemafileUris) {
            try {
                mappers.addAll(createMappers(fileUri));
            } catch (IOException ioe) {
                LOG.warn("error loading avro schema " + fileUri, ioe);
            }
        }

        this.jsonAvroMapperByMimeType =  ImmutableMap.copyOf(mappers.stream().collect(Collectors.toMap(JsonAvroMapper::getMimeType, (JsonAvroMapper m) -> m)));
        this.jsonAvroMapperBySchemaName =  ImmutableMap.copyOf(mappers.stream().collect(Collectors.toMap(JsonAvroMapper::getSchemaName, (JsonAvroMapper m) -> m)));
    }
    
 
    public ImmutableMap<String, JsonAvroMapper> getRegisteredMapper() {
        return jsonAvroMapperByMimeType;
    }
    
    public Optional<JsonAvroMapper> getJsonToAvroMapper(String mimeType) {
        return Optional.ofNullable(jsonAvroMapperByMimeType.get(mimeType));
    }

    public Optional<JsonAvroMapper> getJsonToAvroMapper(SchemaName schemaName) {
        return Optional.ofNullable(jsonAvroMapperBySchemaName.get(schemaName));
    }
    
    
    private ImmutableList<JsonAvroMapper> createMappers(URI schemaURI) throws IOException {
        
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
        

        List<JsonAvroMapper> mappers = Lists.newArrayList();
        for (JsonObject jsonSchema : jsonSchemas) {
            JsonAvroEntityMapper entityMapper = JsonAvroEntityMapper.createrMapper(jsonSchema);
            mappers.add(entityMapper);
            mappers.add(new JsonAvroCollectionMapper(entityMapper));
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

