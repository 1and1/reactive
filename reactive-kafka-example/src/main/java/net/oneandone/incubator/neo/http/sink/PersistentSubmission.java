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
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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
import com.google.common.io.Closeables;

import net.oneandone.incubator.neo.collect.Immutables;
import net.oneandone.incubator.neo.exception.Exceptions;
import net.oneandone.incubator.neo.http.sink.HttpSink.Method;



final class PersistentSubmission extends TransientSubmission {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentSubmission.class);    
    private final SubmissionFile submissionfile;

    private PersistentSubmission(final String id,
                                 final URI target, 
                                 final Method method, 
                                 final Entity<?> entity, 
                                 final ImmutableSet<Integer> rejectStatusList,
                                 final ImmutableList<Duration> retryDelays,
                                 final int numRetries,
                                 final Instant dataLastTrial,
                                 final SubmissionFile submissionfile) {
        super(id, 
              target, 
              method,
              entity, 
              rejectStatusList,
              retryDelays,
              numRetries,
              dataLastTrial);
        
        this.submissionfile = submissionfile;
    }

    private PersistentSubmission(final String id,
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
             new SubmissionFile(createQueryDir(dir, method, target), id));
    } 
    
    private final CompletableFuture<Void> saveAsync() {
        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            new ObjectMapper().writeValue(bos, entity.getEntity());
            
            LOG.debug("saving persistent query " + getId() + " on disc (with pending lock) " + submissionfile.getQueryFile().getAbsolutePath());            
            return MapEncoding.create()
                              .with("id", id)
                              .with("method", method.toString())
                              .with("mediaType", entity.getMediaType().toString())
                              .with("target", target.toString())
                              .with("data", Base64.getEncoder().encodeToString(bos.toByteArray()))
                              .with("retries", Joiner.on("&")
                                                     .join(processDelays.stream()
                                                                        .map(duration -> duration.toMillis())
                                                                        .collect(Immutables.toList())))
                              .with("numTrials", stateRef.get().getNumTrials())
                              .with("dataLastTrial", stateRef.get().getDateLastTrial().toString())
                              .with("rejectStatusList", Joiner.on("&").join(rejectStatusList))
                              .writeToAsync(submissionfile);
        } catch (IOException ioe) {
            return Exceptions.completedFailedFuture(ioe);
        }
    }    
    
    public static CompletableFuture<PersistentSubmission> newPersistentSubmissionAsync(final String id,
                                                                                       final URI target, 
                                                                                       final Method method, 
                                                                                       final Entity<?> entity, 
                                                                                       final ImmutableSet<Integer> rejectStatusList,
                                                                                       final ImmutableList<Duration> retryDelays,
                                                                                       final int numTrials,
                                                                                       final Instant dataLastTrial,
                                                                                       final File dir) {
        try {
            final PersistentSubmission submission = new PersistentSubmission(id,
                                                                             target,
                                                                             method,
                                                                             entity, 
                                                                             rejectStatusList,
                                                                             retryDelays,
                                                                             numTrials,
                                                                             dataLastTrial, 
                                                                             dir);
            return submission.saveAsync()
                             .thenApply((Void) -> submission);  
        } catch (IOException ioe) {
            return Exceptions.completedFailedFuture(ioe);
        }
    }
  
    public File getQueryFile() {
        return submissionfile.getQueryFile();
    }
    
    @Override
    protected void release() {
        super.release();        
        submissionfile.close();
        LOG.debug("persistent query " + getId() + " released " + submissionfile.getQueryFile().getAbsolutePath());                            
    }
    
    @Override
    protected CompletableFuture<Void> updateStateAsync(final FinalState finalState) {
        return super.updateStateAsync(finalState)
                    .thenAccept((Void) -> {
                                                release();
                                                if (submissionfile.delete()) {
                                                    LOG.debug("persistent query " + getId() + " deleted " + submissionfile.getQueryFile().getAbsolutePath());                            
                                                }
                                          });
    }

    @Override
    protected CompletableFuture<Void> updateStateAsync(final PendingState pendingState) {
        LOG.debug("updating persistent query " + getId() + " (numTrials=" + pendingState.getNumTrials() + ") " + submissionfile.getQueryFile().getAbsolutePath());
        return super.updateStateAsync(pendingState)
                    .thenCompose((Void) -> MapEncoding.create()
                                                      .with("numTrials", pendingState.getNumTrials())
                                                      .with("timestampLastTrial", pendingState.getDateLastTrial().toString())
                                                      .writeToAsync(submissionfile));
    }
    
    static File createQueryDir(final File dir, final Method method, final URI target) {
        File queryDir = new File(dir, method + "_" + Base64.getEncoder().encodeToString(target.toString().getBytes(Charsets.UTF_8)).replace("=", ""));
        
        queryDir.mkdirs();
        if (!queryDir.exists()) {
            throw new RuntimeException("could not create query dir " + dir.getAbsolutePath());
        }           
        
        return queryDir;
    }
    
    
    private static final class SubmissionFile implements Closeable {
        private final File queryFile;
        private final AsynchronousFileChannel channel;
        
        SubmissionFile(final File dir, final String id) throws IOException {
            this(new File(dir.getCanonicalFile(), id + ".query"));
        }
        
        SubmissionFile(final File queryFile) throws IOException {
            /*
             * By creating a new persistent query file a file lock will be acquired and not been released
             * until query is completed! This avoids concurrent handling of the same queue (file).
             */
            this.queryFile = queryFile;
            this.channel = AsynchronousFileChannel.open(queryFile.toPath(), StandardOpenOption.CREATE,
                                                                            StandardOpenOption.READ, 
                                                                            StandardOpenOption.WRITE);
            
            if (channel.tryLock() == null) {
                throw new IOException("query file " + queryFile.getAbsolutePath() + " locked by another process");
            }
        }
        
        public CompletableFuture<Void> writeTextAsync(String data) {
            try {
                final CompletableWriteFuture promise = new CompletableWriteFuture();
                channel.write(ByteBuffer.wrap(data.getBytes(Charsets.UTF_8)), channel.size(), null, promise);
                return promise;
            } catch (IOException ioe) {
                return Exceptions.completedFailedFuture(ioe);
            }
        }
        
        private static final class CompletableWriteFuture extends CompletableFuture<Void> implements CompletionHandler<Integer, Object> {
            
            @Override
            public void completed(final Integer result, final Object attachment) {
                super.complete(null);
            }
            
            @Override
            public void failed(final Throwable ex, final Object attachment) {
                super.completeExceptionally(ex);
            }
        }
        

        public CompletableFuture<String> readTextAsync() {
            try {
                final CompletableReadFuture promise = new CompletableReadFuture((int) channel.size());
                channel.read(promise.getBuffer(), 0, null, promise);
                
                return promise.thenApply(binary -> new String(binary, Charsets.UTF_8));
            } catch (IOException ioe) {
                return Exceptions.completedFailedFuture(ioe);
            }
        }
        
        private static final class CompletableReadFuture extends CompletableFuture<byte[]> implements CompletionHandler<Integer, Object> {
            private final ByteBuffer buf;
            
            public CompletableReadFuture(final int sizeToRead) {
                this.buf = ByteBuffer.allocate(sizeToRead);
            }
            
            public ByteBuffer getBuffer() {
                return buf;
            }
            
            @Override
            public void completed(final Integer result, final Object attachment) {
                super.complete(buf.array());
            }
            
            @Override
            public void failed(final Throwable ex, final Object attachment) {
                super.completeExceptionally(ex);
            }
        }
        
        public File getQueryFile() {
            return queryFile;
        }
        
        @Override
        public void close() {
            if (channel.isOpen()) {
                // by closing the file handles the lock will be released
                try {
                    Closeables.close(channel, true);
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
        
        final File submissionDir = createQueryDir(dir, method, target);
        LOG.debug("scanning " + submissionDir.getAbsolutePath() + " for unprocessed query files");
        
        for (File file : readSubmissionFiles(submissionDir)) {
            tryReadFromAsync(file).thenAccept(optionalSubmission -> optionalSubmission.ifPresent(submission -> {
                                                                                                                LOG.debug("old query file " + submission.getId() + " found rescheduling it"); 
                                                                                                                processor.processRetryAsync(submission);
                                                                                                               }));
        }
    }

    private static ImmutableList<File> readSubmissionFiles(final File queryDir) {
        return ImmutableList.copyOf(queryDir.listFiles())
                            .stream()
                            .filter(file -> file.getName().endsWith(".query"))
                            .filter(file -> !Duration.between(Instant.ofEpochMilli(file.lastModified()).plus(Duration.ofSeconds(5)), Instant.now()).isNegative())  // have to be older than 5 sec to avoid race conditions
                            .collect(Immutables.toList());
    } 

    public static CompletableFuture<Optional<PersistentSubmission>> tryReadFromAsync(final File file) {
        try {
            final SubmissionFile submissionFile = new SubmissionFile(file);
            return MapEncoding.readFromAsync(submissionFile)
                              .thenApply(protocol -> deserialize(submissionFile, protocol));
        } catch (IOException | RuntimeException ioe) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
    }

    private static Optional<PersistentSubmission> deserialize(final SubmissionFile submissionFile, final MapEncoding protocol) {
        /*
         * By deserializing the underlying query file a file lock will be acquired 
         * and not been released until query is completed!
         * This avoids concurrent handling of the same queue (file).
         */
        
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
            LOG.warn("No retries left. deleting expired submission file " + submissionFile);
            submissionFile.delete();
            submissionFile.close();
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
                                                        submissionFile));  
        }
    }
  
    private static final class MapEncoding {
        private static final String LINE_SEPARATOR = "\r\n";
        private static final String KEY_VALUE_SEPARATOR = " -> ";
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

        public CompletableFuture<Void> writeToAsync(SubmissionFile file) {
            return file.writeTextAsync(Joiner.on(LINE_SEPARATOR)
                                             .withKeyValueSeparator(KEY_VALUE_SEPARATOR)
                                             .join(data));
        }
        
        public static CompletableFuture<MapEncoding> readFromAsync(SubmissionFile file) {
            return file.readTextAsync()
                       .thenApply(text -> new MapEncoding(ImmutableMap.copyOf(Splitter.on(LINE_SEPARATOR)
                                                                                      .withKeyValueSeparator(KEY_VALUE_SEPARATOR)       
                                                                                      .split(text))));
        }
    }
}        