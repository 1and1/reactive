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
package net.oneandone.commons.incubator.neo.httpsink;

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
import java.util.concurrent.atomic.AtomicLong;
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
import net.oneandone.commons.incubator.neo.collect.Immutables;
import net.oneandone.commons.incubator.neo.httpsink.EntityConsumer.Submission;



/**
 * the http sink provides a consumer-styled API for on-way HTTP upload
 * transactions. By performing such transaction data will be send to the server
 * without getting data back. The http sink provides auto-retries. <br>
 * <br>
 * 
 * Example with in-memory auto-retry
 * 
 * <pre>
 * EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics")
 *                               .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), 
 *                                                                Duration.ofSeconds(3), 
 *                                                                Duration.ofMinutes(1), 
 *                                                                Duration.ofMinutes(30),
 *                                                                Duration.ofHours(5)))
 *                               .open();
 * 
 *  // tries to send data. If fails (5xx) it will be auto-retried. In case of 
 *  // an 4xx an BadRequestException will be thrown 
 *  sink.submitAsync(newCustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json"); 
 *  // ...
 *  
 *  sink.close();
 * <pre>
 * <br>
 * <br>
 * 
 * Example with persistent file-based memory auto-retry
 * 
 * <pre>
 * EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics")
 *                               .withRetryAfter(ImmutableList.of(Duration.ofMillis(100),
 *                                                                Duration.ofSeconds(3), 
 *                                                                Duration.ofMinutes(1),
 *                                                                Duration.ofMinutes(30),
 *                                                                Duration.ofHours(5)))
 *                               .withRetryPersistency(myRetryDir) 
 *                               .open();
 * 
 * // tries to send data. If fails (5xx) it will be auto-retried. In case of an
 * // 4xx an BadRequestException will be thrown 
 * sink.submitAsync(newCustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
 * // ...
 * 
 * sink.close();
 * <pre>
 *
 */
public class HttpSink {
    private static final Logger LOG = LoggerFactory.getLogger(HttpSink.class);

    public enum Method {
        POST, PUT
    };

    private final URI target;
    private final Client client;
    private final Method method;
    private final int bufferSize;
    private final File dir;
    private final String appId;
    private final ImmutableSet<Integer> rejectStatusList;
    private final ImmutableList<Duration> remainingRetries;
    private final int numParallelWorkers;

    private HttpSink(final Client client, 
                     final URI target, 
                     final Method method, 
                     final int bufferSize, 
                     final File dir, 
                     final String appId,
                     final ImmutableSet<Integer> rejectStatusList,
                     final ImmutableList<Duration> remainingRetries, 
                     final int numParallelWorkers) {
        this.client = client;
        this.target = target;
        this.method = method;
        this.bufferSize = bufferSize;
        this.dir = dir;
        this.appId = appId;
        this.rejectStatusList = rejectStatusList;
        this.remainingRetries = remainingRetries;
        this.numParallelWorkers = numParallelWorkers;
    }

    /**
     * @param target the target uri
     * @return a new instance of the http sink
     */
    public static HttpSink create(final String target) {
        return create(URI.create(target));
    }

    /**
     * @param target the target uri
     * @return a new instance of the http sink
     */
    public static HttpSink create(final URI target) {
        return new HttpSink(null, 
                            target, 
                            Method.POST, 
                            Integer.MAX_VALUE,
                            null,
                            null,
                            ImmutableSet.of(400, 403, 405, 406, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417),
                            ImmutableList.of(),
                            1);
    }

    /**
     * @param client the client to use
     * @return a new instance of the http sink
     */
    public HttpSink withClient(final Client client) {
        return new HttpSink(client, 
                            this.target, 
                            this.method, 
                            this.bufferSize, 
                            this.dir, 
                            this.appId,
                            this.rejectStatusList,
                            this.remainingRetries, 
                            this.numParallelWorkers);
    }

    /**
     * @param method the method. Supported are POST and PUT (default is POST)
     * @return a new instance of the http sink
     */
    public HttpSink withMethod(final Method method) {
        return new HttpSink(this.client, 
                            this.target, method, 
                            this.bufferSize, 
                            this.dir,
                            this.appId,
                            this.rejectStatusList,
                            this.remainingRetries,
                            this.numParallelWorkers);
    }

    /**
     * @param retryPauses the delays before retrying (Default is empty list)
     * @return a new instance of the http sink
     */
    public HttpSink withRetryAfter(final ImmutableList<Duration> retryPauses) {
        return new HttpSink(this.client, 
                            this.target, 
                            this.method,
                            this.bufferSize, 
                            this.dir, 
                            this.appId,
                            this.rejectStatusList,
                            retryPauses,
                            this.numParallelWorkers);
    }

    /**
     * @param numParallelWorkers the parallelity by performing retries (default is 1)
     * @return a new instance of the http sink
     */
    public HttpSink withRetryParallelity(final int numParallelWorkers) {
        return new HttpSink(this.client,
                            this.target, 
                            this.method, 
                            this.bufferSize,
                            this.dir,
                            this.appId,
                            this.rejectStatusList,
                            this.remainingRetries,
                            numParallelWorkers);
    }

    /**
     * @param bufferSize the retry buffer size. If the size is exceeded, new retry jobs will be discarded (default is unlimited)
     * @return a new instance of the http sink
     */
    public HttpSink withRetryBufferSize(final int bufferSize) {
        return new HttpSink(this.client, 
                            this.target, 
                            this.method, 
                            bufferSize, 
                            this.dir,
                            this.appId,
                            this.rejectStatusList,
                            this.remainingRetries,
                            this.numParallelWorkers);
    }

    /**
     * @param dir the directory where the retry jobs will be stored. If not set,
     *            the retry jobs will be stored in-memory (default is unset)
     * @return a new instance of the http sink
     */
    public HttpSink withRetryPersistency(final File dir) {
        return new HttpSink(this.client, 
                            this.target, 
                            this.method, 
                            this.bufferSize, 
                            dir,
                            this.appId,
                            this.rejectStatusList,
                            this.remainingRetries, 
                            this.numParallelWorkers);
    }

    
    /**
     * 
     * @param rejectStatusList the set of status codes which will not initiate a retry. Instead a runtime exception will be 
     *                         thrown, if such a response status is received (default [400, 403, 405, 406, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417])
     * @return a new instance of the http sink
     */
    public HttpSink withRejectOnStatus(final ImmutableSet<Integer> rejectStatusList) {
        return new HttpSink(this.client, 
                            this.target, 
                            this.method, 
                            this.bufferSize, 
                            this.dir,
                            this.appId,
                            rejectStatusList,
                            this.remainingRetries, 
                            this.numParallelWorkers);
    }

    
    /**
     * @return the sink reference
     */
    public EntityConsumer open() {
        return new QueryQueue();
    }

    private boolean isPersistentQueue() {
        return dir != null;
    }
    

    private final class QueryQueue implements EntityConsumer, Closeable {
        private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(numParallelWorkers);
        private final AtomicReference<Client> defaultClientRef = new AtomicReference<>();
        
        // statistics
        private final AtomicLong success = new AtomicLong(0);
        private final AtomicLong retries = new AtomicLong(0);
        private final AtomicLong discarded = new AtomicLong(0);
        private final AtomicLong rejected = new AtomicLong(0);
        
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
        public long getNumSuccess() {
            return success.get();
        }

        @Override
        public long getNumRetries() {
            return retries.get();
        }

        @Override
        public long getNumRejected() {
            return rejected.get();
        }

        @Override
        public long getNumDiscarded() {
            return discarded.get();
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

            final CompletableFuture<Submission> promise = new CompletableFuture<>();
            final Query query = (dir == null) ?  new Query(target, 
                                                           method, 
                                                           appId,
                                                           Entity.entity(entity, mediaType), 
                                                           remainingRetries)
                                              : new PersistentQuery(target, 
                                                                    method, 
                                                                    appId, 
                                                                    Entity.entity(entity, mediaType), 
                                                                    remainingRetries,
                                                                    dir);
            execute(query, promise);

            return promise;
        }

        
        private void execute(final Query query, final CompletableFuture<Submission> promise) {

            query.executeWith(getClient()).whenComplete((Void, error) -> {
                
                                        try {
                        
                                            // success
                                            if (error == null) {
                                                query.close(true);
                                                success.incrementAndGet();
                                                promise.complete(query);
                        
                                            // error
                                            } else {
                                                int status = (error instanceof WebApplicationException) ? ((WebApplicationException) error).getResponse().getStatus() 
                                                                                                        : 500;
                                                
                                                // rejecting error?
                                                if (rejectStatusList.contains(status)) {
                                                    LOG.debug("got client error complete exceptionally", error);
                                                    query.close(false);
                                                    rejected.incrementAndGet();
                                                    promise.completeExceptionally(error);
                                                    
                                                // ..no try reschedule    
                                                } else {
                                                    LOG.warn("query failed: " + query);
                                                    Optional<Duration> nextExecutionDelay = query.nextExecutionDelay();
                                                    if (nextExecutionDelay.isPresent()) {
                                                        LOG.warn("retry query after " + nextExecutionDelay.get());
                                                        executor.schedule(() -> executeRetry(query), 
                                                                          nextExecutionDelay.get().toMillis(),
                                                                          TimeUnit.MILLISECONDS);
                                                    } else {                                                    
                                                        LOG.warn("no retries left! discard query " + query);
                                                        discarded.incrementAndGet();
                                                        query.close(false);
                                                    }

                                                    promise.complete(query);
                                                }
                                            }
                        
                                        } catch (RuntimeException rt) {
                                            promise.completeExceptionally(rt);
                                        }
            });
        }
        
        
        void executeRetry(final Query query) {
            retries.incrementAndGet();
            execute(query, new CompletableFuture<>());  
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
                                                                  LOG.info("persistent query found " + query.getQueryFile().getAbsolutePath() + 
                                                                           " retry query after " + nextExecutionDelay.get());
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
        protected final Method method;
        protected final URI target;
        protected final String appId;
        protected final Entity<?> entity;
        protected final AtomicReference<EntityConsumer.Submission.Status> status = new AtomicReference<>(EntityConsumer.Submission.Status.PENDING);
        protected final AtomicReference<ImmutableList<Duration>> remainingRetrysRef;
        
        public Query(final URI target, 
                     final Method method, 
                     final String appId,
                     final Entity<?> entity, 
                     final ImmutableList<Duration> remainingRetrys) {
            Preconditions.checkNotNull(target);
            Preconditions.checkNotNull(method);
            Preconditions.checkNotNull(entity);
            
            this.target = target;
            this.method = method;
            this.appId = appId;
            this.entity = entity;
            this.remainingRetrysRef = new AtomicReference<>(remainingRetrys);
        }

        public boolean isOpen() {
            return (getStatus() == EntityConsumer.Submission.Status.PENDING);
        }
        
        public void close(boolean isSuccess) {
            status.set(isSuccess ? EntityConsumer.Submission.Status.COMPLETED : EntityConsumer.Submission.Status.DISCARDED);
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
            if (appId != null) {
                builder.header("X-APP", appId);
            }

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
    
    
    
    private static final class PersistentQuery extends Query {
        private final File queryFile;
        private final RandomAccessFile raf;
        private final FileChannel channel;
        private final Writer writer;

        
        private PersistentQuery(final File queryFile, 
                                final RandomAccessFile raf, 
                                final FileChannel channel, 
                                final Writer writer, 
                                final URI target, 
                                final Method method, 
                                final String appId,
                                final Entity<?> entity, 
                                final ImmutableList<Duration> remainingRetrys) {
            super(target, method, appId, entity, remainingRetrys);
            
            Preconditions.checkNotNull(queryFile);
            Preconditions.checkState(channel.isOpen());
            
            this.queryFile = queryFile;
            this.raf = raf;
            this.channel = channel;
            this.writer = writer;
        }
        
        public PersistentQuery(final URI target, 
                               final Method method, 
                               final String appId,
                               final Entity<?> entity, 
                               final ImmutableList<Duration> remainingRetrys, 
                               final File dir) {
            super(target, method, appId, entity, remainingRetrys);
            
            try {
                queryFile = new File(dir.getCanonicalFile(), UUID.randomUUID().toString() + ".query");
                raf = new RandomAccessFile(queryFile, "rw");
                channel = raf.getChannel();
                channel.lock();                          // get lock. lock will not been release until query is completed! 
                writer = Channels.newWriter(channel, Charsets.UTF_8.toString());

                
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                new ObjectMapper().writeValue(bos, entity.getEntity());
                
                MapEncoding.create()
                           .with("appId", appId)
                           .with("method", method.toString())
                           .with("mediaType", entity.getMediaType().toString())
                           .with("target", target.toString())
                           .with("class", entity.getEntity().getClass().getName())
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
                    MapEncoding.create()
                              .with("retries", "") 
                              .writeTo(writer);

                    writer.close();
                    channel.close();
                    raf.close();
                    
                    queryFile.delete();
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
                    String appId = protocol.get("appid");
                    Method method = protocol.get("method", txt -> Method.valueOf(txt));
                    URI target = protocol.get("target", txt -> URI.create(txt));
                    MediaType mediaType = protocol.get("mediaType", txt -> MediaType.valueOf(txt));
                    String clazz = protocol.get("class");
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
                        return Optional.of(new PersistentQuery(queryFile,
                                                               raf, 
                                                               channel, 
                                                               writer,
                                                               target,
                                                               method, 
                                                               appId, 
                                                               Entity.entity(new ObjectMapper().readValue(data, Class.forName(clazz)), mediaType),
                                                               retries));
                    }
                }
                
            } catch (IOException | ClassNotFoundException | RuntimeException e) {
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
