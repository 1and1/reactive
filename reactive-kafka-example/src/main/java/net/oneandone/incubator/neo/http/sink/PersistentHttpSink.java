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
package net.oneandone.incubator.neo.http.sink;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Function;


import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;

import net.oneandone.incubator.neo.collect.Immutables;



final class PersistentHttpSink extends TransientHttpSink {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentHttpSink.class);

    private final File queryDir;
    
    public PersistentHttpSink(final Client client, 
                              final URI target, 
                              final Method method, 
                              final int bufferSize,  
                              final ImmutableSet<Integer> rejectStatusList,
                              final ImmutableList<Duration> remainingRetries, 
                              final int numParallelWorkers,
                              final File dir) {
        super(client, 
              target, 
              method, 
              bufferSize,
              rejectStatusList,
              remainingRetries, 
              numParallelWorkers);
        this.queryDir = createQueryDir(dir, method, target);

        // create dir if not already exists
        queryDir.mkdirs();
        if (!queryDir.exists()) {
            throw new RuntimeException("could not create query dir " + dir.getAbsolutePath());
        }           
        
        // looks for old jobs and process it
        readUnlockedPersistentJobs().forEach(query -> tryReschedule(query));            
    }
    
    static File createQueryDir(final File dir, final Method method, final URI target) {
        return new File(dir, method + "_" + Base64.getEncoder().encodeToString(target.toString().getBytes(Charsets.UTF_8)).replace("=", ""));
    }
    
    private ImmutableList<PersistentQuery> readUnlockedPersistentJobs() {
        return ImmutableList.copyOf(queryDir.listFiles())
                            .stream()
                            .filter(file -> file.getName().endsWith(".query"))
                            .map(file -> PersistentQuery.tryReadFrom(file, super.client, super.executor, super.monitor))
                            .filter(optionalQuery -> optionalQuery.isPresent())
                            .map(optionalQuery -> optionalQuery.get())
                            .collect(Immutables.toList());
    } 
    
    private final void tryReschedule(PersistentQuery query) {
        LOG.warn("found uncompleted persistent query " + query.getId() + " try to rescheduling it");
        query.tryReschedule();
    }

    public File getQueryDir() {
        return queryDir;
    }
    
    @Override
    public CompletableFuture<Submission> submitAsync(Object entity, String mediaType) {
        return PersistentQuery.openAsync(super.client, 
                                         super.target, 
                                         super.method, 
                                         Entity.entity(entity, mediaType),
                                         super.rejectStatusList,
                                         super.retryDelays,
                                         super.executor,
                                         super.monitor,
                                         queryDir)
                              .thenApply(query -> (Submission) query);
    }    
    
    static class PersistentQuery extends Query {
        private final FileRef fileRef;

        
        private PersistentQuery(final Client client, 
                                final String id,
                                final URI target, 
                                final Method method, 
                                final Entity<?> entity, 
                                final ImmutableSet<Integer> rejectStatusList,
                                final ImmutableList<Duration> retryDelays,
                                final int numRetries,
                                final ScheduledThreadPoolExecutor executor,
                                final Monitor monitor,
                                final FileRef fileRef) {
            super(client, 
                  id, 
                  target, 
                  method,
                  entity, 
                  rejectStatusList,
                  retryDelays,
                  numRetries,
                  executor,
                  monitor);
            
            this.fileRef = fileRef;
        }

        protected PersistentQuery(final Client client, 
                                  final String id,
                                  final URI target, 
                                  final Method method, 
                                  final Entity<?> entity, 
                                  final ImmutableSet<Integer> rejectStatusList,
                                  final ImmutableList<Duration> retryDelays,
                                  final int numRetries,
                                  final ScheduledThreadPoolExecutor executor,
                                  final Monitor monitor,
                                  final File queryDir) throws IOException {
            this(client, 
                 id, 
                 target, 
                 method,
                 entity, 
                 rejectStatusList,
                 retryDelays,
                 numRetries,
                 executor,
                 monitor,
                 new FileRef(queryDir, UUID.randomUUID().toString()));
         
            try {
                final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                new ObjectMapper().writeValue(bos, entity.getEntity());
                
                MapEncoding.create()
                           .with("id", id)
                           .with("method", method.toString())
                           .with("mediaType", entity.getMediaType().toString())
                           .with("target", target.toString())
                           .with("data", Base64.getEncoder().encodeToString(bos.toByteArray()))
                           .with("retries", Joiner.on("&")
                                                  .join(retryDelays.stream()
                                                                   .map(duration -> duration.toMillis())
                                                                   .collect(Immutables.toList())))
                           .with("numRetries", numRetries)
                           .with("rejectStatusList", Joiner.on("&").join(rejectStatusList))
                           .writeTo(this.fileRef.getWriter());
                
                LOG.debug("persistent query " + getId() + " save on disc (with pendding lock) " + fileRef.getQueryFile().getAbsolutePath());
                
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        } 

        
        public static Optional<PersistentQuery> tryReadFrom(final File queryFile, 
                                                            final Client client,    
                                                            final ScheduledThreadPoolExecutor executor,  
                                                            final Monitor monitor) {
            
            /*
             * By deserializing the underlying query file a file lock will be acquired 
             * and not been released until query is completed!
             * This avoids concurrent handling of the same queue (file).
             */

            FileRef fileRef = null;
            try {
                fileRef = new FileRef(queryFile);
                    
                final MapEncoding protocol = MapEncoding.readFrom(fileRef.getReader());
                final String id = protocol.get("id");
                final Method method = protocol.get("method", txt -> Method.valueOf(txt));
                final URI target = protocol.get("target", txt -> URI.create(txt));
                final MediaType mediaType = protocol.get("mediaType", txt -> MediaType.valueOf(txt));
                final byte[] data = protocol.get("data", txt -> Base64.getDecoder().decode(txt));
                final ImmutableList<Duration> retryDelays = protocol.get("retries", txt -> Strings.isNullOrEmpty(txt) ? ImmutableList.<Duration>of()  
                                                                                                                      : Splitter.on("&")
                                                                                                                                .trimResults()
                                                                                                                                .splitToList(txt)
                                                                                                                                .stream()
                                                                                                                                .map(millis -> Duration.ofMillis(Long.parseLong(millis)))
                                                                                                                                .collect(Immutables.toList()));
                final int numRetries = protocol.getInt("numRetries");
                final ImmutableSet<Integer> rejectStatusList = Splitter.on("&")
                                                                       .trimResults()
                                                                       .splitToList(protocol.get("rejectStatusList"))
                                                                       .stream()
                                                                       .map(status -> Integer.parseInt(status))
                                                                       .collect(Immutables.toSet());
                
                // no retries left?
                if (numRetries >= retryDelays.size()) {
                    LOG.warn("No retries left. deleting expired query file " + queryFile);
                    fileRef.delete();
                    fileRef.close();
                    return Optional.empty();
                
                // retries are available; return query (lock will not been released to avoid parallel processing of the query file) 
                } else {
                    return Optional.of(new PersistentQuery(client, 
                                                           id, 
                                                           target, 
                                                           method, 
                                                           Entity.entity(data, mediaType), 
                                                           rejectStatusList, 
                                                           retryDelays, 
                                                           numRetries,
                                                           executor, 
                                                           monitor, 
                                                           fileRef));  
                }
                
            } catch (IOException | RuntimeException e) {
                LOG.warn("query file " + queryFile.getAbsolutePath() + " seems to be logged or corrupt. ignoring it");
                if (fileRef != null) {
                    fileRef.close();
                }
                
                return Optional.empty();
            }
        }
        
        
        private static final class FileRef implements Closeable {
            private final File queryFile;
            private final RandomAccessFile raf;
            private final FileChannel channel;
            private final Writer writer;
            private final LineNumberReader reader;
            
            FileRef(final File dir, final String id) throws IOException {
                this(new File(dir.getCanonicalFile(), id + ".query"));
            }
            
            FileRef(final File queryFile) throws IOException {
                /*
                 * By creating a new persistent query file a file lock will be acquired and not been released
                 * until query is completed! This avoids concurrent handling of the same queue (file).
                 */
                
                this.queryFile = queryFile;
                this.raf = new RandomAccessFile(queryFile, "rw");
                this.channel = raf.getChannel();
                
                if (channel.tryLock() == null) {
                    throw new IOException("query file " + queryFile.getAbsolutePath() + " locked by another process");
                }
                
                this.writer = Channels.newWriter(channel, Charsets.UTF_8.toString());
                this.reader = new LineNumberReader(Channels.newReader(channel, Charsets.UTF_8.toString()));
            }
            
            public Writer getWriter() {
                return writer;
            }
            
            public LineNumberReader getReader() {
                return reader;
            }
            
            public File getQueryFile() {
                return queryFile;
            }
            
            @Override
            public void close() {
                if (channel.isOpen()) {
                    // by closing the file handles the lock will be released
                    try {
                        Closeables.close(writer, true);
                        Closeables.close(reader, true);
                        Closeables.close(channel, true);
                        Closeables.close(raf, true);
                    } catch (IOException ioe) {
                        LOG.debug("error occured by closing query file " + queryFile.getAbsolutePath(), ioe);
                    }
                }
            }     
            
            public boolean delete() {
                close();
                return queryFile.delete();
            }
        }
        
        
        public static CompletableFuture<Query> openAsync(final Client client,
                                                         final URI target, 
                                                         final Method method,    
                                                         final Entity<?> entity,
                                                         final ImmutableSet<Integer> rejectStatusList,
                                                         final ImmutableList<Duration> remainingRetrys,
                                                         final ScheduledThreadPoolExecutor executor,
                                                         final Monitor monitor,
                                                         final File queryDir) {
            try  {
                final PersistentQuery  query = new PersistentQuery(client,
                                                                   UUID.randomUUID().toString(), 
                                                                   target, 
                                                                   method,   
                                                                   entity,
                                                                   rejectStatusList,
                                                                   remainingRetrys,
                                                                   0,
                                                                   executor,  
                                                                   monitor,
                                                                   queryDir);

                LOG.debug("submitting persistent query" + query);
                return query.process();
            } catch (IOException ioe) {
                throw new RuntimeException("could not create query file: " + ioe.getMessage());
            }
        }
        
        
        public File getQueryFile() {
            return fileRef.getQueryFile();
        }
        
        @Override
        protected void setNumRetries(int numRetries) {
            try {
                MapEncoding.create()
                           .with("numRetries", numRetries)
                           .writeTo(fileRef.getWriter());
                
                LOG.debug("persistent query " + getId() + " updated (numRetries=" + numRetries + ") " + fileRef.getQueryFile().getAbsolutePath());
                
            } catch (RuntimeException ignore) { }

            super.setNumRetries(numRetries);
        }
        
        @Override
        protected void release() {
            fileRef.close();
            if (!isRetriesLeft()) {
                boolean isDeleted = fileRef.delete();
                if (isDeleted) {
                    LOG.debug("persistent query " + getId() + " deleted " + fileRef.getQueryFile().getAbsolutePath());                            
                }
                return;
            }
                
            LOG.debug("persistent query " + getId() + " released " + fileRef.getQueryFile().getAbsolutePath());                            
            super.release();
        }
    }            

    private static final class MapEncoding {
        private final ImmutableMap<String, String> data;
        
        private MapEncoding(ImmutableMap<String, String> data) {
            this.data = data;
        }
        
        public static MapEncoding create() {
            return new MapEncoding(ImmutableMap.of());
        }
        
        public MapEncoding with(String name, int value) {
            return with(name, Integer.toString(value));
        }
        
        public MapEncoding with(String name, String value) {
            if (value == null) {
                return this;
            } else {
                return new MapEncoding(Immutables.join(data, name, value));
            }
        }

        public int getInt(String name) {
            return Integer.parseInt(get(name));
        }

        public String get(String name) {
            return get(name, obj -> obj.toString());
        }

        public <T> T get(String name, Function<String, T> mapper) {
            String txt = data.get(name);
            if (txt == null) {
                return null;
            } else {
                return mapper.apply(txt);
            }
        }

        public void writeTo(Writer writer) {
            try {
                for (Entry<String, String> entry : data.entrySet()) {
                    writer.write(entry.getKey() + ": " + entry.getValue() + "\r\n");
                }
                writer.flush();
            } catch (IOException ioe)  {
                throw new RuntimeException(ioe);
            }
        }
        
        public static MapEncoding readFrom(LineNumberReader reader) {
            try {
                Map<String, String> map = Maps.newHashMap();
                String line = null;
                do {
                    line = reader.readLine();
                    if (line != null) {
                        int idx = line.indexOf(":");
                        if (idx > 0) {
                            map.put(line.substring(0,  idx), line.substring(idx + 1, line.length()).trim());
                        }
                    }
                } while (line != null); 
                
                return new MapEncoding(ImmutableMap.copyOf(map));
            } catch (IOException ioe)  {
                throw new RuntimeException(ioe);
            }
        }
    }
}