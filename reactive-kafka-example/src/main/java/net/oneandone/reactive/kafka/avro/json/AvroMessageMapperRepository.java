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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.json.Json;
import javax.json.JsonObject;
import javax.ws.rs.core.MediaType;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;

import jersey.repackaged.com.google.common.collect.Lists;
import net.oneandone.incubator.neo.collect.Immutables;
import net.oneandone.incubator.neo.datareplicator.DataReplicator;
import net.oneandone.incubator.neo.datareplicator.ReplicationJob;
import net.oneandone.reactive.kafka.KafkaMessageIdList;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.utils.Pair;





public class AvroMessageMapperRepository implements Closeable {
    
    private static final Logger LOG = LoggerFactory.getLogger(AvroMessageMapperRepository.class);

    private final ReplicationJob replicationJob; 
    private volatile ImmutableList<SchemaInfo> erroneousSchemas = ImmutableList.of();
    private volatile ImmutableMap<String, AvroMessageMapper> mapper = ImmutableMap.of();
    private volatile ImmutableMap<String, String> schemaNameIndex = ImmutableMap.of();
    private volatile ImmutableMap<String, Schema> schemaIndex = ImmutableMap.of();

    
    
    
    public AvroMessageMapperRepository(final URI uri) {
        this.replicationJob = DataReplicator.create(uri)
                                            .withRefreshPeriod(Duration.ofMinutes(1))
                                            .startConsumingBinary(this::onRefresh);
    }
 
    
    public void onRefresh(final byte[] binary) {
        
        ImmutableSet<Pair<AvroMessageMapper, SchemaInfo>> results = parseSchemas(binary).stream()
                                                                                        .map(schema -> AvroMessageMapper.createrMapper(schema))
                                                                                        .collect(Immutables.toSet());

        
        erroneousSchemas = results.stream()
                                  .filter(result -> (result.getFirst() == null))
                                  .map(result -> result.getSecond())
                                  .collect(Immutables.toList());

        
        this.mapper = results.stream()
                             .filter(result -> (result.getFirst() != null))
                             .map(result -> result.getFirst())
                             .collect(Immutables.toMap(AvroMessageMapper::getMimeType,
                                      (AvroMessageMapper m) -> m));
        
        this.schemaNameIndex = mapper.values()
                                     .stream()
                                     .collect(Immutables.toMap(m -> m.getSchema().getFullName(), 
                                                               m -> m.getMimeType()));
        
        this.schemaIndex = mapper.values()
                                 .stream()
                                 .collect(Immutables.toMap(m -> m.getSchema().getFullName(), 
                                                           m -> m.getSchema()));
    }    
    
    
 
    
    @Override
    public void close() throws IOException {
        replicationJob.close();
    }


    public ImmutableMap<String, String> getRegisteredSchemasAsText() {
        final Map<String, String> schemas = Maps.newHashMap();
        
        for (Entry<String, AvroMessageMapper> entry : mapper.entrySet()) {
            schemas.put(entry.getKey(), entry.getValue().toString());
            schemas.put(entry.getKey().replace("+json", ".list+json"), "[" + entry.getValue().toString() + "\r\n]");
        }
        
        return ImmutableMap.copyOf(schemas);
    }
    

    public  ImmutableMap<String, Schema> getRegisteredSchemas() {
        return mapper.entrySet().stream().collect(Immutables.toMap(e -> e.getKey(), e -> e.getValue().getSchema()));
    }
    
    
    public ImmutableList<SchemaInfo> getErroneousSchemas() {
        return erroneousSchemas;
    }

    
    public Optional<String> getRegisteredSchema(final MediaType mimetype) {
        for (Entry<String, String> entry : getRegisteredSchemasAsText().entrySet()) {
            if (entry.getKey().equals(mimetype.toString())) {
                return Optional.of(entry.getValue().toString());
            }
        }
        
        return Optional.empty();
    }
    

    public JsonObject toJson(final AvroMessage avroMessage) {
        return getJsonToAvroMapper(avroMessage.getSchema()).map(mapper -> mapper.toJson(avroMessage))
                                                           .orElseThrow(() -> new SchemaException("unsupported type", avroMessage.getMimeType().toString()));
    }

    
    public ImmutableList<AvroMessage> toAvroMessages(final InputStream jsonObjectStream, final String mimeType) {
        return getJsonToAvroMapper(mimeType).map(mapper -> mapper.toAvroMessages(Json.createParser(jsonObjectStream)))
                                            .orElseThrow(() -> new SchemaException("unsupported type", mimeType));
    }
    

    public AvroMessage toAvroMessage(final InputStream jsonObjectStream, final String mimeType) {
        return getJsonToAvroMapper(mimeType).map(mapper -> mapper.toAvroMessage(Json.createParser(jsonObjectStream)))
                                            .orElseThrow(() -> new SchemaException("unsupported type", mimeType));
    }
    

    public AvroMessage toAvroMessage(final byte[] serializedAvroMessage, final MediaType readerMimeType) throws SchemaException {
        return toAvroMessage(serializedAvroMessage, ImmutableList.of(readerMimeType));
    }
    

    public AvroMessage toAvroMessage(final byte[] serializedAvroMessage, final ImmutableList<MediaType> readerMimeTypes) throws SchemaException {
        final ImmutableList<Schema> readerSchemas = readerMimeTypes.stream()
                                                                   .map(mimeType -> getJsonToAvroMapper(mimeType.toString()))
                                                                   .filter(optionalMapper -> optionalMapper.isPresent())
                                                                   .map(mapper -> mapper.get().getSchema())
                                                                   .collect(Immutables.toList());
        
        final AvroMessage avroMessage = AvroMessage.from(serializedAvroMessage, schemaIndex, readerSchemas);

        for (MediaType readerMimeType : readerMimeTypes) {
            if (avroMessage.getMimeType().isCompatible(readerMimeType)) {
                return avroMessage;
            }
        }

        throw new SchemaException("avro messaqge type " + avroMessage.getMimeType() + " does not match with a requested type " + readerMimeTypes);
    }
    

    public ServerSentEvent toServerSentEvent(final KafkaMessageIdList consumedOffsets, final AvroMessage avroMessage) {
        return getJsonToAvroMapper(avroMessage.getSchema()).map(schema -> ServerSentEvent.newEvent()
                                                                                         .id(consumedOffsets.toString())
                                                                                         .data(avroMessage.getMimeType().toString() + "\n" + toJson(avroMessage).toString()))
                                                           .orElseThrow(() -> new SchemaException("unsupported type", avroMessage.getMimeType().toString()));
    }
    
    
    Schema getSchema(final String namespace, final String name) {
        return getJsonToAvroMapper(namespace, name).orElseThrow(() -> new SchemaException("unsupported type", namespace + "." + name))
                                                   .getSchema();
    }
    
    
    private Optional<AvroMessageMapper> getJsonToAvroMapper(final Schema schema) {
        return getJsonToAvroMapper(schema.getNamespace(), schema.getName());
    }
    

    private Optional<AvroMessageMapper> getJsonToAvroMapper(final String namespace, final String name) {
        return  Optional.ofNullable(schemaNameIndex.get(namespace + "." + name))
                        .map(mimeType -> mapper.get(mimeType));
    }

    
    private Optional<AvroMessageMapper> getJsonToAvroMapper(final String mimeType) {
        return Optional.ofNullable(mapper.get(mimeType));
    }
    
   
    

    
    private static ImmutableList<SchemaInfo> parseSchemas(final byte[] binary) {
        try {
            // unpack within temp dir 
            final Path tempDir = Files.createTempDirectory("avro_repo_" + new Random().nextInt(Integer.MAX_VALUE) + "_");
            Zip.unpack(binary, tempDir);

            
            // scan avro scheme files
            final AvroSchemeFileVisitor avroSchemeFileVisitor = new AvroSchemeFileVisitor();
            Files.walkFileTree(tempDir, avroSchemeFileVisitor);

            // scan avro idl files
            final AvroIdlFileVisitor avroIdlFileVisitor = new AvroIdlFileVisitor();
            Files.walkFileTree(tempDir, avroIdlFileVisitor);

            
            // return schema sets
            return Immutables.join(avroSchemeFileVisitor.getCollectedSchemas(),
                                   avroIdlFileVisitor.getCollectedSchemas())
                             .stream()
                             .distinct()
                             .collect(Immutables.toList());
            
            
            
        } catch (IOException ioe) {
            LOG.warn("error loading avro schema", ioe);
            return ImmutableList.of();
        }
    }
    
    
    
    private static final class AvroSchemeFileVisitor extends SimpleFileVisitor<Path> {
        private final List<SchemaInfo> schemas = Lists.newArrayList();

        public ImmutableList<SchemaInfo> getCollectedSchemas() {
            return ImmutableList.copyOf(schemas);
        }
        
        @Override
        public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs) throws IOException {
            
            if (path.getFileName().toString().endsWith(".avsc")) {
                
            //    try (InputStream is = new FileInputStream(path.toFile())) {
                    schemas.add(new SchemaInfo(path.toFile().getAbsolutePath(), 
                                attrs.lastModifiedTime().toInstant(), 
                                new Schema.Parser().parse(path.toFile())));
            //    }
            }
                
            return FileVisitResult.CONTINUE;
        }
    }
    
    
    private static final class AvroIdlFileVisitor extends SimpleFileVisitor<Path> {
        private final List<SchemaInfo> schemas = Lists.newArrayList();

        public ImmutableList<SchemaInfo> getCollectedSchemas() {
            return ImmutableList.copyOf(schemas);
        }
        
        @Override
        public FileVisitResult visitFile(final Path path, final BasicFileAttributes attrs) throws IOException {
            
            if (path.getFileName().toString().endsWith(".avdl")) {
                
                //try (InputStream is = new FileInputStream(path.toFile())) {
    
                try {
                    //for (Schema schema : idlToJsonSchemas(is)) {
                    for (Schema schema : idlToJsonSchemas(path.toFile())) {
                        schemas.add(new SchemaInfo(path.toFile().getAbsolutePath(), 
                                    attrs.lastModifiedTime().toInstant(), 
                                    schema));
                    }
                } catch (ParseException | IOException e) {
                    throw new RuntimeException("error occured by parsing " + path.toFile().getAbsolutePath(), e);
                }
            }
                
            return FileVisitResult.CONTINUE;
        }
        
        
        private ImmutableSet<Schema> idlToJsonSchemas(final File file) throws IOException, ParseException {
            try (Idl parser = new Idl(file)) {
                return parser.CompilationUnit()
                             .getTypes()
                             .stream()
                             .filter(scheme -> (scheme.getType() == Type.RECORD))
                             .collect(Immutables.toSet());

            } 
        }            
    }
    
 
    
    private static class Zip {

        private Zip() { }
        
        public static void unpack(final byte[] binary, final Path targetDir) throws IOException {
            final ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(binary));

            ZipEntry ze = null;
            while ((ze = zin.getNextEntry()) != null) {
                final File file = new File(targetDir + File.separator + ze.getName());
                if (ze.isDirectory()) {
                    file.mkdir();
                } else {
                    file.toPath().getParent().toFile().mkdirs();
                    Files.write(file.toPath(), ByteStreams.toByteArray(zin));
                }
                zin.closeEntry();
            }
            zin.close();
        }
    }
}