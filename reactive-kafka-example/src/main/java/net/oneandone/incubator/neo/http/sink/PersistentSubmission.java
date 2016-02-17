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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;
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
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

import net.oneandone.incubator.neo.collect.Immutables;
import net.oneandone.incubator.neo.http.sink.HttpSink.Method;



final class PersistentSubmission extends TransientSubmission {
    private static final String SUBMISSION_SUFFIX = ".submission";
    private static final String TEMP_SUFFIX = ".temp";
    private static final String DELETED_SUFFIX = ".deleted";
    private static final Logger LOG = LoggerFactory.getLogger(PersistentSubmission.class);    
    private final File submissionDir;
    
    private PersistentSubmission(final String id,
                                 final URI target, 
                                 final Method method, 
                                 final Entity<?> entity, 
                                 final ImmutableSet<Integer> rejectStatusList,
                                 final ImmutableList<Duration> retryDelays,
                                 final int numRetries,
                                 final Instant dataLastTrial,
                                 final File submissionDir) {
        super(id, 
              target, 
              method,
              entity, 
              rejectStatusList,
              retryDelays,
              numRetries,
              dataLastTrial);
        
        this.submissionDir = submissionDir;
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
            final PersistentSubmission submission = new PersistentSubmission(id,
                                                                             target,
                                                                             method,
                                                                             entity, 
                                                                             rejectStatusList,
                                                                             retryDelays,
                                                                             numTrials,
                                                                             dataLastTrial, 
                                                                             new File(dir, id));
            return CompletableFuture.supplyAsync(() -> save(submission)); 
    }
  
    @Override
    protected Void onCompleted() {
        deletePersistentSubmission();
        return null;
    }
    
    @Override
    protected Void onUpdated() {
        LOG.debug("updating persistent query " + getId() + " (numTrials=" + stateRef.get().getNumTrials() + ") ");
        save(PersistentSubmission.this);
        return null;
    }

    @Override
    protected void onReleased() {
        // TODO release lock
        LOG.debug("persistent submission " + getId() + " released " + submissionDir.getAbsolutePath());                            
    }
    
    static File createQueryDir(final File dir, final Method method, final URI target) {
        File queryDir = new File(dir, method + "_" + Base64.getEncoder().encodeToString(target.toString().getBytes(Charsets.UTF_8)).replace("=", ""));
        
        queryDir.mkdirs();
        if (!queryDir.exists()) {
            throw new RuntimeException("could not create query dir " + dir.getAbsolutePath());
        }           
        
        return queryDir;
    }
    
    public static void processOldQueryFiles(final File dir,
                                            final Method method, 
                                            final URI target, 
                                            final Processor processor) {
        
        final File submissionDir = createQueryDir(dir, method, target);
        LOG.debug("scanning " + submissionDir.getAbsolutePath() + " for unprocessed query files");
        
        for (File submissionFile : readSubmissionFiles(submissionDir)) {
            PersistentSubmission submission = load(submissionFile);
            LOG.debug("old submission file " + submission.getId() + " found rescheduling it"); 
            processor.processRetryAsync(submission);
        }
    }

    private static ImmutableList<File> readSubmissionFiles(final File submissionsDir) {
        return ImmutableList.copyOf(submissionsDir.listFiles())
                            .stream()
                            .map(submissionDir -> getFile(submissionDir))
                            .filter(optionalSubmissionFile -> optionalSubmissionFile.isPresent()) 
                            .map(optionalSubmissionFile -> optionalSubmissionFile.get())
                            .collect(Immutables.toList());
    } 

    File getSubmissionDir() {
        return submissionDir;
    }
            
    public Optional<File> getFile() {
        return getFile(submissionDir);
    }
    
    private static Optional<File> getFile(final File submissionDir) {
        if (getDeleteMarkerFile(submissionDir).exists()) {
            return Optional.empty();
        } else {
            Optional<File> submissionFile = Optional.empty();
            Instant newest = Instant.ofEpochMilli(0); 
            
            for (File file : submissionDir.listFiles()) {
                final String name = file.getName(); 
                if (name.endsWith(SUBMISSION_SUFFIX)) {
                    Instant time = Instant.ofEpochMilli(Long.parseLong(name.substring(0, SUBMISSION_SUFFIX.length())));
                    if (time.isAfter(newest)) {
                        newest = time;
                        submissionFile = Optional.of(file);
                    }
                }
            }
            
            return submissionFile;
        }
    }
    
    private final static File getDeleteMarkerFile(final File submissionDir) {
        return new File(submissionDir, "submission" + DELETED_SUFFIX);
    }
    
    private void deletePersistentSubmission() {
        // write deleted marker
        File deletedFile = getDeleteMarkerFile(submissionDir) ;
        try {
            deletedFile.createNewFile();
        } catch (IOException ignore) { }
        
        // try to deleted all file (may fail for exceptional reason)
        boolean filesDeleted = true;
        for (File file : submissionDir.listFiles()) {
            if (!file.getAbsolutePath().equals(deletedFile.getAbsolutePath())) {
                filesDeleted = filesDeleted && file.delete();
            }
        }
        
        if (filesDeleted) {
            deletedFile.delete();
            if (submissionDir.delete()) {
                LOG.debug("persistent query " + getId() + " deleted " + submissionDir.getAbsolutePath());                            
            }
        }
        
    }
    
    private static final PersistentSubmission save(final PersistentSubmission submission) {
        if (!submission.submissionDir.exists()) {
            submission.submissionDir.mkdirs();
        }
        
        final File tempFile = new File(submission.submissionDir, UUID.randomUUID().toString() + TEMP_SUFFIX);
        try {
            FileOutputStream os = null;
            try {
                // write the new cache file
                os = new FileOutputStream(tempFile);
                
                final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                new ObjectMapper().writeValue(bos, submission.entity.getEntity());
                
                MapEncoding.create()
                           .with("id", submission.id)
                           .with("method", submission.method.toString())
                           .with("mediaType", submission.entity.getMediaType().toString())
                           .with("target", submission.target.toString())
                           .with("data", Base64.getEncoder().encodeToString(bos.toByteArray()))
                           .with("retries", Joiner.on("&")
                                                  .join(submission.processDelays.stream()
                                                                                .map(duration -> duration.toMillis())
                                                                                .collect(Immutables.toList())))
                           .with("numTrials", submission.stateRef.get().getNumTrials())
                           .with("dataLastTrial", submission.stateRef.get().getDateLastTrial().toString())
                           .with("rejectStatusList", Joiner.on("&").join(submission.rejectStatusList))
                           .writeTo(os);
                os.close();

                // and commit it (this renaming approach avoids "half-written" files)
                final File submissionFile = new File(submission.submissionDir, Instant.now().toEpochMilli() + SUBMISSION_SUFFIX);
                submissionFile.createNewFile();
                java.nio.file.Files.move(tempFile.toPath(), submissionFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
                LOG.debug("persistent query " + submission.id + " saved on disc (with pending lock) " + submissionFile.getAbsolutePath());  
            } finally {
                Closeables.close(os, true);  // close os in any case
            }

            // perform clean up to remove crashed temp file
            //cleanup();

        } catch (final IOException ioe) {
            LOG.debug("saving persistent submission " + submission.id + " failed", ioe);
            throw new RuntimeException(ioe);
        }
        
        return submission;
    }    
    
    private static final PersistentSubmission load(final File submissionFile) {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(submissionFile);
            final MapEncoding protocol = MapEncoding.readFrom(fis);
            
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
            return new PersistentSubmission(id, 
                                            target, 
                                            method, 
                                            Entity.entity(data, mediaType), 
                                            rejectStatusList, 
                                            retryDelays, 
                                            numTrials,
                                            dateLastTrial,
                                            submissionFile.getParentFile());
        } catch (final IOException ioe) {
            LOG.debug("loading persistent submission " + submissionFile.getAbsolutePath() + " failed", ioe);
            throw new RuntimeException(ioe);
        } finally {
            Closeables.closeQuietly(fis);
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

        public void writeTo(final OutputStream os) throws IOException {
            final String txt = Joiner.on(LINE_SEPARATOR)
                                     .withKeyValueSeparator(KEY_VALUE_SEPARATOR)
                                     .join(data);
            os.write(txt.getBytes(Charsets.UTF_8));
        }
        
        public static MapEncoding readFrom(final InputStream is) throws IOException {
            final String txt = new String(ByteStreams.toByteArray(is), Charsets.UTF_8);
            return new MapEncoding(ImmutableMap.copyOf(Splitter.on(LINE_SEPARATOR)
                                               .withKeyValueSeparator(KEY_VALUE_SEPARATOR)       
                                               .split(txt)));
        }
    }
}        