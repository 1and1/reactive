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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.ResponseProcessingException;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;

import joptsimple.internal.Strings;
import net.oneandone.incubator.neo.collect.Immutables;
import net.oneandone.incubator.neo.exception.Exceptions;
import net.oneandone.incubator.neo.http.sink.HttpSink.Submission;



final class HttpSinkBuilderImpl implements HttpSinkBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkBuilderImpl.class);

    private final URI target;
    private final Client client;
    private final Method method;
    private final int bufferSize;
    private final File dir;
    private final ImmutableSet<Integer> rejectStatusList;
    private final ImmutableList<Duration> remainingRetries;
    private final int numParallelWorkers;

    
    HttpSinkBuilderImpl(final Client client, 
                        final URI target, 
                        final Method method, 
                        final int bufferSize, 
                        final File dir, 
                        final ImmutableSet<Integer> rejectStatusList,
                        final ImmutableList<Duration> remainingRetries, 
                        final int numParallelWorkers) {
        this.client = client;
        this.target = target;
        this.method = method;
        this.bufferSize = bufferSize;
        this.dir = dir;
        this.rejectStatusList = rejectStatusList;
        this.remainingRetries = remainingRetries;
        this.numParallelWorkers = numParallelWorkers;
    }



    @Override
    public HttpSinkBuilder withClient(final Client client) {
        Preconditions.checkNotNull(client);
        return new HttpSinkBuilderImpl(client, 
                                this.target, 
                                this.method, 
                                this.bufferSize, 
                                this.dir, 
                                this.rejectStatusList,
                                this.remainingRetries, 
                                this.numParallelWorkers);
    }

    @Override
    public HttpSinkBuilder withMethod(final Method method) {
        Preconditions.checkNotNull(method);
        return new HttpSinkBuilderImpl(this.client, 
                                this.target, method, 
                                this.bufferSize, 
                                this.dir,
                                this.rejectStatusList,
                                this.remainingRetries,
                                this.numParallelWorkers);
    }

    @Override
    public HttpSinkBuilder withRetryAfter(final ImmutableList<Duration> retryPauses) {
        Preconditions.checkNotNull(retryPauses);
        return new HttpSinkBuilderImpl(this.client, 
                                this.target, 
                                this.method,
                                this.bufferSize, 
                                this.dir, 
                                this.rejectStatusList,
                                retryPauses,
                                this.numParallelWorkers);
    }
    
    @Override
    public HttpSinkBuilder withRetryAfter(final Duration... retryPauses) {
        Preconditions.checkNotNull(retryPauses);
        return withRetryAfter(ImmutableList.copyOf(retryPauses));
    }
    
    @Override
    public HttpSinkBuilder withRetryParallelity(final int numParallelWorkers) {
        return new HttpSinkBuilderImpl(this.client,
                                this.target, 
                                this.method, 
                                this.bufferSize,
                                this.dir,
                                this.rejectStatusList,
                                this.remainingRetries,
                                numParallelWorkers);
    }

    @Override
    public HttpSinkBuilder withRetryBufferSize(final int bufferSize) {
        return new HttpSinkBuilderImpl(this.client, 
                                this.target, 
                                this.method, 
                                bufferSize, 
                                this.dir,
                                this.rejectStatusList,
                                this.remainingRetries,
                                this.numParallelWorkers);
    }

    @Override
    public HttpSinkBuilder withRetryPersistency(final File dir) {
        Preconditions.checkNotNull(dir);
        return new HttpSinkBuilderImpl(this.client, 
                                this.target, 
                                this.method, 
                                this.bufferSize, 
                                dir,
                                this.rejectStatusList,
                                this.remainingRetries, 
                                this.numParallelWorkers);
    }

    @Override
    public HttpSinkBuilder withRejectOnStatus(final ImmutableSet<Integer> rejectStatusList) {
        Preconditions.checkNotNull(rejectStatusList);
        return new HttpSinkBuilderImpl(this.client, 
                                this.target, 
                                this.method, 
                                this.bufferSize, 
                                this.dir,
                                rejectStatusList,
                                this.remainingRetries, 
                                this.numParallelWorkers);
    }

    
    /**
     * @return the sink reference
     */
    public HttpSink open() {
        return new QueryQueue();
    }

    private boolean isPersistentQueue() {
        return dir != null;
    }
    

    private final class QueryQueue implements HttpSink, Metrics, Closeable {
        private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(numParallelWorkers);
        private final AtomicReference<Client> defaultClientRef = new AtomicReference<>();
        
        // statistics
        private final MetricRegistry metrics = new MetricRegistry();
        private final Counter success = metrics.counter("success");
        private final Counter retries = metrics.counter("retries");
        private final Counter discarded = metrics.counter("discarded");
        private final Counter rejected = metrics.counter("rejected");
        
        
        public QueryQueue() {
            if (isPersistentQueue()) {
                new PersistentQueryScanner().run();
            }
        }
        
        
        @Override
        public void close() {
            final Client defaultClient = defaultClientRef.get();
            if (defaultClient != null) {
                defaultClient.close();
            }
            
            executor.shutdown();
        }
        
        
        @Override
        public Metrics getMetrics() {
            return this;
        }
        
        @Override
        public Counter getNumSuccess() {
            return success;
        }

        @Override
        public Counter getNumRetries() {
            return retries;
        }

        @Override
        public Counter getNumRejected() {
            return rejected;
        }

        @Override
        public Counter getNumDiscarded() {
            return discarded;
        }

        @Override
        public int getQueueSize() {
            return 0;
        }

        private Client getClient() {
            if (client == null) {
                synchronized (this) {
                    Client defaultClient = defaultClientRef.get();
                    if (defaultClient == null) {
                        defaultClient = ClientBuilder.newClient();
                        defaultClientRef.set(defaultClient);
                    }
                    return defaultClient;
                }
            } else {
                return client;
            }
        }

        
        @Override
        public CompletableFuture<Submission> submitAsync(final Object entity, final String mediaType) {
            if (isPersistentQueue() && !(mediaType.startsWith("application") && mediaType.endsWith("json"))) {
                throw new IllegalStateException("persistent retry supports JSON types only");
            }

            final Query query = (dir == null) ?  new Query(target, 
                                                           method, 
                                                           Entity.entity(entity, mediaType), 
                                                           remainingRetries)
                                              : new PersistentQuery(target, 
                                                                    method, 
                                                                    Entity.entity(entity, mediaType), 
                                                                    remainingRetries,
                                                                    dir);
            
            LOG.debug("query " + query.getId() + " submitted: " + query);
            return execute(query);
        }

        
        private CompletableFuture<Submission> execute(final Query query) {

            return query.executeWith(getClient())
                        .thenApply(Void -> { 
                                              success.inc();
                                              query.close(true);
                                              return (Submission) query;
                                    })
                        .exceptionally(error -> {
                                                  error = Exceptions.unwrap(error);
                                                  int status = (error instanceof WebApplicationException) ? ((WebApplicationException) error).getResponse().getStatus() 
                                                                                                          : 500;

                                                  // rejecting error?
                                                  if (rejectStatusList.contains(status)) {
                                                      LOG.debug("got client error for query " + query.getId() + " completing exceptionally", error);
                                                      query.close(false);
                                                      rejected.inc();
                                                      throw Exceptions.propagate(error);
                                        
                                                  // ..no try reschedule    
                                                  } else {
                                                      LOG.warn("query " + query.getId() + " failed: " + query);
                                                      Optional<Duration> nextExecutionDelay = query.nextExecutionDelay();
                                                      if (nextExecutionDelay.isPresent()) {
                                                          LOG.warn("will retry query " + query.getId() + " after " + nextExecutionDelay.get());
                                                          executor.schedule(() -> executeRetry(query), 
                                                                            nextExecutionDelay.get().toMillis(),
                                                                            TimeUnit.MILLISECONDS);
                                                      } else {                                                    
                                                          LOG.warn("no retries left for " + query.getId() + " discarding query");
                                                          discarded.inc();
                                                          query.close(false);
                                                      }
                                                      return (Submission) query;
                                                }
                                    });
        }
        
        
        void executeRetry(final Query query) {
            retries.inc();
            
            execute(query).thenAccept(submission -> { 
                                                        if (submission.getStatus() == Submission.Status.COMPLETED) { 
                                                            LOG.warn("retry for query " + query.getId() + " was succesfully");
                                                        }
                                                    });
        }
        
        
        
        private final class PersistentQueryScanner implements Runnable { 

            @Override
            public void run() {
                
                for (File file : ImmutableList.copyOf(dir.listFiles())) {
                    if (file.getName().endsWith(".query")) {
                        PersistentQuery.readFrom(file)
                                       .ifPresent(query -> {
                                                              Optional<Duration> nextExecutionDelay = query.nextExecutionDelay();
                                                              if (nextExecutionDelay.isPresent()) {
                                                                  LOG.info("query  " + query.getId() + " found (" + query.getQueryFile().getAbsolutePath() + 
                                                                           ") Retrying it after " + nextExecutionDelay.get());
                                                                  executor.schedule(() -> executeRetry(query), 
                                                                                          nextExecutionDelay.get().toMillis(),
                                                                                          TimeUnit.MILLISECONDS);
                                                              }
                                                           });
                    }
                }
            }
        }
    }



    
    private static class Query implements Submission {
        protected final String id;
        protected final Method method;
        protected final URI target;
        protected final Entity<?> entity;
        protected final AtomicReference<HttpSink.Submission.Status> status = new AtomicReference<>(HttpSink.Submission.Status.PENDING);
        protected final AtomicReference<ImmutableList<Duration>> remainingRetrysRef;
        
        
        public Query(final URI target, 
                     final Method method, 
                     final Entity<?> entity, 
                     final ImmutableList<Duration> remainingRetrys) {
            this(UUID.randomUUID().toString(), target, method, entity, remainingRetrys);
        }
        
        protected Query(final String id,
                        final URI target, 
                        final Method method,    
                        final Entity<?> entity, 
                        final ImmutableList<Duration> remainingRetrys) {

            Preconditions.checkNotNull(id);
            Preconditions.checkNotNull(target);
            Preconditions.checkNotNull(method);
            Preconditions.checkNotNull(entity);
            this.id = id;
            this.target = target;
            this.method = method;
            this.entity = entity;
            this.remainingRetrysRef = new AtomicReference<>(remainingRetrys);
        }

        public boolean isOpen() {
            return (getStatus() == HttpSink.Submission.Status.PENDING);
        }
        
        public void close(boolean isSuccess) {
            status.set(isSuccess ? HttpSink.Submission.Status.COMPLETED : HttpSink.Submission.Status.DISCARDED);
        }

        public String getId() {
            return id;
        }
        
        @Override
        public Status getStatus() {
            return status.get();
        }
        
        public Optional<Duration> nextExecutionDelay() {
            final ImmutableList<Duration> remaingDelays = remainingRetrysRef.get();
            if (remaingDelays.isEmpty()) {
                return Optional.empty();
            } else {
                ImmutableList<Duration> remainingRetrys = remainingRetrysRef.get();
                updateRemaingDelays(remainingRetrys.subList(1, remainingRetrys.size()));
                return  Optional.of(remainingRetrys.get(0));
            }
        }
        
        public CompletableFuture<Void> executeWith(Client client) {
            final ResponseHandler responseHandler = new ResponseHandler();

            final Builder builder = client.target(target).request();
            if (method == Method.POST) {
                builder.async().post(entity, responseHandler);
            } else {
                builder.async().put(entity, responseHandler);
            }

            return responseHandler;
        }

        private final class ResponseHandler extends CompletableFuture<Void> implements InvocationCallback<String> {

            @Override
            public void failed(Throwable ex) {
                if (ex instanceof ResponseProcessingException) {
                    ex = ((ResponseProcessingException) ex).getCause();
                }
                completeExceptionally(ex);
            }

            @Override
            public void completed(String responseEntity) {
                updateRemaingDelays(ImmutableList.of());
                complete(null);
            }
        }

        protected void updateRemaingDelays(ImmutableList<Duration> newRemainingRetrys) {
            remainingRetrysRef.set(newRemainingRetrys);
        }

        @Override
        public String toString() {
            return method + " " + target.toString();
        }
    }
    
    
    
    static final class PersistentQuery extends Query {
        private final File queryFile;
        private final RandomAccessFile raf;
        private final FileChannel channel;
        private final Writer writer;

        
        private PersistentQuery(final String id,
                                final File queryFile, 
                                final RandomAccessFile raf, 
                                final FileChannel channel, 
                                final Writer writer, 
                                final URI target, 
                                final Method method, 
                                final Entity<?> entity, 
                                final ImmutableList<Duration> remainingRetrys) {
            super(id, target, method, entity, remainingRetrys);
            
            Preconditions.checkNotNull(queryFile);
            Preconditions.checkState(channel.isOpen());
            
            this.queryFile = queryFile;
            this.raf = raf;
            this.channel = channel;
            this.writer = writer;
        }
        
        public PersistentQuery(final URI target, 
                               final Method method, 
                               final Entity<?> entity, 
                               final ImmutableList<Duration> remainingRetrys, 
                               final File dir) {
            super(target, method, entity, remainingRetrys);
            
            try {
                queryFile = new File(dir.getCanonicalFile(), UUID.randomUUID().toString() + ".query");
                raf = new RandomAccessFile(queryFile, "rw");
                channel = raf.getChannel();
                channel.lock();                          // get lock. lock will not been release until query is completed! 
                writer = Channels.newWriter(channel, Charsets.UTF_8.toString());

                
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                new ObjectMapper().writeValue(bos, entity.getEntity());
                
                MapEncoding.create()
                           .with("id", id)
                           .with("method", method.toString())
                           .with("mediaType", entity.getMediaType().toString())
                           .with("target", target.toString())
                           .with("data", Base64.getEncoder().encodeToString(bos.toByteArray()))
                           .with("retries", Joiner.on("&")
                                                  .join(remainingRetrys.stream()
                                                                       .map(duration -> duration.toMillis())
                                                                       .collect(Immutables.toList())))
                           .writeTo(writer);
                
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
        
        
        public File getQueryFile() {
            return queryFile;
        }
        
        @Override
        protected void updateRemaingDelays(ImmutableList<Duration> newRemaingDelays) {
            super.updateRemaingDelays(newRemaingDelays);

            if (isOpen()) {
                MapEncoding.create()
                          .with("retries", Joiner.on("&").join(newRemaingDelays.stream().map(duration -> duration.toMillis()).collect(Immutables.toList()))) 
                          .writeTo(writer);
                
                if (newRemaingDelays.isEmpty()) {
                    close(false);
                }
            }
        }

        
        @Override
        public void close(boolean isSuccess) {
            
            try {
                if (isOpen()) {
                    Closeables.close(writer, true);
                    Closeables.close(channel, true);
                    Closeables.close(raf, true);

                    if (isSuccess || remainingRetrysRef.get().isEmpty()) {
                        queryFile.delete();
                    }
                }
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            
            super.close(isSuccess);
        }
        

        public static Optional<PersistentQuery> readFrom(File queryFile) {

            RandomAccessFile raf = null;
            FileChannel channel = null;

            try {
                raf = new RandomAccessFile(queryFile, "rw");
                channel = raf.getChannel();
                
                if (channel.tryLock() == null) {
                    LOG.debug("query file " + queryFile.getAbsolutePath() + " is locked. ignoring it");
                    raf.close();
                    return Optional.empty();
                    
                } else {
                    LineNumberReader reader = new LineNumberReader(Channels.newReader(channel, Charsets.UTF_8.toString()));
                    Writer writer = Channels.newWriter(channel, Charsets.UTF_8.toString());
                    
                    MapEncoding protocol = MapEncoding.readFrom(reader);
                    String id = protocol.get("id");
                    Method method = protocol.get("method", txt -> Method.valueOf(txt));
                    URI target = protocol.get("target", txt -> URI.create(txt));
                    MediaType mediaType = protocol.get("mediaType", txt -> MediaType.valueOf(txt));
                    byte[] data = protocol.get("data", txt -> Base64.getDecoder().decode(txt));
                    ImmutableList<Duration> retries = protocol.get("retries", txt -> Strings.isNullOrEmpty(txt) ? ImmutableList.<Duration>of()  
                                                                                                                : Splitter.on("&")
                                                                                                                .trimResults()
                                                                                                                .splitToList(txt)
                                                                                                                .stream()
                                                                                                                .map(milisString -> Duration.ofMillis(Long.parseLong(milisString)))
                                                                                                                .collect(Immutables.<Duration>toList()));
    
                                                                                      
           
                    if (retries.isEmpty()) {
                        LOG.warn("deleting expired query file " + queryFile);
                        raf.close();
                        queryFile.delete();
                        return Optional.empty();
                        
                    } else {
                        return Optional.of(new PersistentQuery(id,  
                                                               queryFile,
                                                               raf, 
                                                               channel, 
                                                               writer,
                                                               target,
                                                               method, 
                                                               Entity.entity(data, mediaType),
                                                               retries));
                    }
                }
                
            } catch (IOException | RuntimeException e) {
                LOG.warn("query file " + queryFile.getAbsolutePath() + " seems to be corrupt. ignoring it");
                
                try {
                    Closeables.close(channel, true);
                    Closeables.close(raf, true);
                } catch (IOException ignore) { }
                
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
            
            public MapEncoding with(String name, String value) {
                if (value == null) {
                    return this;
                } else {
                    return new MapEncoding(Immutables.join(data, name, value));
                }
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
}