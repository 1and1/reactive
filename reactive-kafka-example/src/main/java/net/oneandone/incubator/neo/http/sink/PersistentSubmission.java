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
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;


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
import net.oneandone.incubator.neo.http.sink.HttpSink.Method;



final class PersistentSubmission extends TransientSubmission {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentSubmission.class);
    
    private final FileRef fileRef;

    private PersistentSubmission(final String id,
                                 final URI target, 
                                 final Method method, 
                                 final Entity<?> entity, 
                                 final ImmutableSet<Integer> rejectStatusList,
                                 final ImmutableList<Duration> retryDelays,
                                 final int numRetries,
                                 final Instant dataLastTrial,
                                 final FileRef fileRef) {
        super(id, 
              target, 
              method,
              entity, 
              rejectStatusList,
              retryDelays,
              numRetries,
              dataLastTrial);
        
        this.fileRef = fileRef;
    }

    protected PersistentSubmission(final String id,
                                   final URI target, 
                                   final Method method, 
                                   final Entity<?> entity, 
                                   final ImmutableSet<Integer> rejectStatusList,
                                   final ImmutableList<Duration> retryDelays,
                                   final int numTrials,
                                   final Instant dataLastTrial,
                                   final File dir) throws IOException {
        this(id, 
             target, 
             method,
             entity, 
             rejectStatusList,
             retryDelays,
             numTrials,
             dataLastTrial,
             new FileRef(createQueryDir(dir, method, target), id));
     
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
                       .with("numTrials", numTrials)
                       .with("dataLastTrial", dataLastTrial.toString())
                       .with("rejectStatusList", Joiner.on("&").join(rejectStatusList))
                       .writeTo(this.fileRef.getWriter());
            
            LOG.debug("persistent query " + getId() + " save on disc (with pending lock) " + fileRef.getQueryFile().getAbsolutePath());
            
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    } 
    
    public File getQueryFile() {
        return fileRef.getQueryFile();
    }
    
    
    @Override
    protected void release() {
        super.release();
        
        fileRef.close();
        LOG.debug("persistent query " + getId() + " released " + fileRef.getQueryFile().getAbsolutePath());                            
    }

    
    @Override
    protected void updateState(FinalState finalState) {
        super.updateState(finalState);
        release();
        
        if (fileRef.delete()) {
            LOG.debug("persistent query " + getId() + " deleted " + fileRef.getQueryFile().getAbsolutePath());                            
        }
    }

    @Override
    protected void updateState(PendingState pendingState) {
        super.updateState(pendingState);
        try {
            MapEncoding.create()
                       .with("numTrials", pendingState.getNumTrials())
                       .with("timestampLastTrial", pendingState.getDateLastTrial().toString())
                       .writeTo(fileRef.getWriter());
            
            LOG.debug("persistent query " + getId() + " updated (numTrials=" + pendingState.getNumTrials() + ") " + fileRef.getQueryFile().getAbsolutePath());
        } catch (RuntimeException ignore) { }
    }
    
   


    static File createQueryDir(final File dir, final Method method, final URI target) {
        File queryDir = new File(dir, method + "_" + Base64.getEncoder().encodeToString(target.toString().getBytes(Charsets.UTF_8)).replace("=", ""));
        
        queryDir.mkdirs();
        if (!queryDir.exists()) {
            throw new RuntimeException("could not create query dir " + dir.getAbsolutePath());
        }           
        
        return queryDir;
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
     
    public static void processOldQueryFiles(final File dir,
                                        final Method method, 
                                        final URI target, 
                                        final Processor processor) { 
        readUnlockedPersistentJobs(createQueryDir(dir, method, target)).forEach(query -> { LOG.debug("old query file " + query.getId() + " found rescheduling it"); processor.processRetry(query); });            
    }
    
    private static ImmutableList<PersistentSubmission> readUnlockedPersistentJobs(final File queryDir) {
        LOG.debug("scanning " + queryDir.getAbsolutePath() + " for unprocessed query files");
        return ImmutableList.copyOf(queryDir.listFiles())
                            .stream()
                            .filter(file -> file.getName().endsWith(".query"))
                            .map(file -> tryReadFrom(file))
                            .filter(optionalQuery -> optionalQuery.isPresent())
                            .map(optionalQuery -> optionalQuery.get())
                            .collect(Immutables.toList());
    } 
    
    
    public static Optional<PersistentSubmission> tryReadFrom(final File queryFile) {
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
            final int numTrials = protocol.getInt("numTrials");
            final Instant dateLastTrial = Instant.parse(protocol.get("dataLastTrial"));
            final ImmutableSet<Integer> rejectStatusList = Splitter.on("&")
                                                                   .trimResults()
                                                                   .splitToList(protocol.get("rejectStatusList"))
                                                                   .stream()
                                                                   .map(status -> Integer.parseInt(status))
                                                                   .collect(Immutables.toSet());
            
            // no retries left?
            if (numTrials > retryDelays.size()) {
                LOG.warn("No retries left. deleting expired query file " + queryFile);
                fileRef.delete();
                fileRef.close();
                return Optional.empty();
            
            // retries are available; return query (lock will not been released to avoid parallel processing of the query file) 
            } else {
                return Optional.of(new PersistentSubmission(id, 
                                                            target, 
                                                            method, 
                                                            Entity.entity(data, mediaType), 
                                                            rejectStatusList, 
                                                            retryDelays, 
                                                            numTrials,
                                                            dateLastTrial,
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
  
    private static final class MapEncoding {
        private final ImmutableMap<String, String> data;
        
        private MapEncoding(ImmutableMap<String, String> data) {
            this.data = data;
        }
        
        public static MapEncoding create() {
            return new MapEncoding(ImmutableMap.of());
        }
        
        public MapEncoding with(String name, long value) {
            return with(name, Long.toString(value));
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