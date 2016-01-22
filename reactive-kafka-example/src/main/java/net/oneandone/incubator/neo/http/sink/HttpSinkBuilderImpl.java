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
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;

import net.oneandone.incubator.neo.collect.Immutables;
import net.oneandone.incubator.neo.exception.Exceptions;


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

        // if dir is set, sink will run in persistent mode  
        return (dir == null) ? new TransientQueryQueue(client,                 // queries will be stored in-memory only      
                                                       target, 
                                                       method, 
                                                       bufferSize, 
                                                       rejectStatusList, 
                                                       remainingRetries,
                                                       numParallelWorkers)               
                             : new PersistentQueryQueue(client,                // queries will be stored on file system as well
                                                        target, 
                                                        method,
                                                        bufferSize, 
                                                        rejectStatusList,
                                                        remainingRetries,
                                                        numParallelWorkers, 
                                                        dir);    
    }

    
    
    
    static interface QueryLifeCycleListener {
        
        void onQueryCompleted(TransientQueryQueue.Query query, boolean isSuccess);
        
        void onQueryCreated(TransientQueryQueue.Query query);
    }
    
    


    
    static class TransientQueryQueue implements HttpSink, QueryLifeCycleListener, Metrics, Closeable {
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        private final ImmutableSet<Integer> rejectStatusList;
        private final Optional<Client> clientToClose;
        protected final Client client;
        protected final ScheduledThreadPoolExecutor executor;
        protected final URI target;
        protected final Method method;
        protected final ImmutableList<Duration> remainingRetries;

        protected final Set<Query> runningQuery = Collections.newSetFromMap(new WeakHashMap<Query, Boolean>());
        
        // statistics
        private final MetricRegistry metrics = new MetricRegistry();
        private final Counter success = metrics.counter("success");
        private final Counter retries = metrics.counter("retries");
        private final Counter discarded = metrics.counter("discarded");
        private final Counter rejected = metrics.counter("rejected");

        
        protected TransientQueryQueue(final Client client, 
                                      final URI target, 
                                      final Method method, 
                                      final int bufferSize, 
                                      final ImmutableSet<Integer> rejectStatusList,
                                      final ImmutableList<Duration> remainingRetries, 
                                      final int numParallelWorkers) {
            this.target = target;
            this.method = method;
            this.rejectStatusList = rejectStatusList;
            this.remainingRetries = remainingRetries;
            this.executor = new ScheduledThreadPoolExecutor(numParallelWorkers);
    
            // using default client?
            if (client == null) {
                final Client defaultClient = ClientBuilder.newClient();
                this.client = defaultClient;
                this.clientToClose = Optional.of(defaultClient);
                
            // .. no client is given by user 
            } else {
                this.client = client;
                this.clientToClose = Optional.empty();
            }
        }
        
        @Override
        public boolean isOpen() {
            return isOpen.get();
        }
        
        @Override
        public void close() {
            if (isOpen.getAndSet(false)) {
                clientToClose.ifPresent(client -> client.close());
                executor.shutdown();
            }
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
            return executor.getQueue().size();
        }
        
        @Override
        public CompletableFuture<Submission> submitAsync(final Object entity, final String mediaType) {
            final Query query = newQuery(entity, mediaType);
            LOG.debug("submitting query " + query.getId() + " " + query);
            return submitAsync(query);
        }
        
        private Query newQuery(final Object entity, final String mediaType) {
            final Query query = new Query(target, method, Entity.entity(entity, mediaType), this, remainingRetries);
            onQueryCreated(query);
            return query;
        }
        
        protected CompletableFuture<Submission> submitAsync(final Query query) {
            if (!isOpen()) {
                throw new IllegalStateException("queue is alreday closed. query " + query.getId() + " will not enqueued");
            }
            
            return query.executeWith(client)
                        .thenApply(Void -> {      
                                                  // success   
                                                  success.inc();
                                                  return (Submission) query;
                                           })
                        .exceptionally(error -> {
                                                  // error
                                                  LOG.warn("query " + query.getId() + " failed: " + query);

                                                  // rejecting error?
                                                  if (rejectStatusList.contains(readAssociatedStatus(error))) {
                                                      LOG.debug("got client error for query " + query.getId() + " completing exceptionally", error);
                                                      rejected.inc();
                                                      throw Exceptions.propagate(error);                                        
                                                  // ..no     
                                                  } else {
                                                      tryReschedule(query);
                                                      return (Submission) query;
                                                }
                                    });
        }
        
        void tryReschedule(final Query query) {
            final Optional<Duration> nextExecutionDelay = query.getNextExecutionDelay();

            // retries left?
            if (nextExecutionDelay.isPresent()) {
                LOG.debug("will retry query " + query.getId() + " after " + nextExecutionDelay.get());

                final Runnable delayedTask = () -> {
                                                    submitAsync(query).thenAccept(submission -> LOG.debug("retry for query " + query.getId() + " was " +
                                                                                                          ((submission.getState() == Submission.State.COMPLETED) ? "" : "not ") + 
                                                                                                          "succesfully"));
                                                    query.removeNextExecutionDelay();  // query has been submitted -> decrement retries    
                                                    retries.inc();
                };
                executor.schedule(delayedTask, nextExecutionDelay.get().toMillis(), TimeUnit.MILLISECONDS);                
               
            // no 
            } else {
                discarded.inc();
            }            
        }
        
        private static int readAssociatedStatus(final Throwable t) { 
            final Throwable error = Exceptions.unwrap(t);
            return (error instanceof WebApplicationException) ? ((WebApplicationException) error).getResponse().getStatus() 
                                                              : 500;
        }        

        @Override
        public void onQueryCreated(Query query) {
            synchronized (runningQuery) {
                runningQuery.add(query);
            }
        }
        
        @Override
        public void onQueryCompleted(Query query, boolean isSuccess) {
            synchronized (runningQuery) {
                runningQuery.remove(query);
            }
        }
   
        protected ImmutableList<Query> drainRunningQueries() {
            ImmutableList<Query> pendings;
            synchronized (runningQuery) {
                pendings = ImmutableList.copyOf(runningQuery);
                runningQuery.clear();
            }
            return pendings;
        }
        
        protected static class Query implements Submission {
            protected final QueryLifeCycleListener queryLifeCycleListener;
            protected final String id;
            protected final Method method;
            protected final URI target;
            protected final Entity<?> entity;
            protected final AtomicReference<HttpSink.Submission.State> stateRef = new AtomicReference<>(HttpSink.Submission.State.PENDING);
            protected final AtomicReference<ImmutableList<Duration>> remainingRetrysRef;
            
            
            public Query(final URI target, 
                         final Method method, 
                         final Entity<?> entity,
                         final QueryLifeCycleListener queryLifeCycleListener,
                         final ImmutableList<Duration> remainingRetrys) {
                this(UUID.randomUUID().toString(), target, method, entity, queryLifeCycleListener, remainingRetrys);
            }
            
            protected Query(final String id,
                            final URI target, 
                            final Method method,    
                            final Entity<?> entity,
                            final QueryLifeCycleListener queryLifeCycleListener,
                            final ImmutableList<Duration> remainingRetrys) {
                Preconditions.checkNotNull(id);
                Preconditions.checkNotNull(target);
                Preconditions.checkNotNull(method);
                Preconditions.checkNotNull(entity);
                
                this.id = id;
                this.target = target;
                this.method = method;
                this.entity = entity;
                this.queryLifeCycleListener = queryLifeCycleListener;
                this.remainingRetrysRef = new AtomicReference<>(remainingRetrys);
            }

            public String getId() {
                return id;
            }
            
            @Override
            public State getState() {
                return stateRef.get();
            }
            
            Optional<Duration> getNextExecutionDelay() {
                final ImmutableList<Duration> remaingDelays = remainingRetrysRef.get();
                if (remaingDelays.isEmpty()) {
                    return Optional.empty();
                } else {
                    return Optional.of(remaingDelays.get(0));
                }
            }
            
            boolean removeNextExecutionDelay() {
                final ImmutableList<Duration> remaingDelays = remainingRetrysRef.get();
                if (remaingDelays.isEmpty()) {
                    return false;
                } else {
                    final ImmutableList<Duration> remainingRetrys = remainingRetrysRef.get();
                    updateRemaingDelays(remainingRetrys.subList(1, remainingRetrys.size()));
                    return true;
                }
            }
            
            public CompletableFuture<Void> executeWith(Client client) {
                final ResponseHandler responseHandler = new ResponseHandler();

                try {
                    final Builder builder = client.target(target).request();
                    if (method == Method.POST) {
                        builder.async().post(entity, responseHandler);
                    } else {
                        builder.async().put(entity, responseHandler);
                    }
                } catch (RuntimeException rt) {
                    responseHandler.failed(rt);
                }

                return responseHandler;
            }

            private final class ResponseHandler extends CompletableFuture<Void> implements InvocationCallback<String> {

                @Override
                public void failed(final Throwable ex) {
                    
                    // no retries left?
                    if (!getNextExecutionDelay().isPresent()) {
                        LOG.warn("no retries left for " + getId() + " discarding query");
                        updateState(HttpSink.Submission.State.DISCARDED);
                    }

                    final Throwable error;
                    if (ex instanceof ResponseProcessingException) {
                        error = ((ResponseProcessingException) ex).getCause();
                    } else {
                        error = ex;
                    }
                    completeExceptionally(error);
                }

                @Override
                public void completed(String responseEntity) {
                    updateState(State.COMPLETED);
                    complete(null);
                }
            }
            
            private void updateState(State newState) {
                if ((newState == State.COMPLETED) || (newState == State.DISCARDED)) {
                    updateRemaingDelays(ImmutableList.of());
                }
                
                stateRef.set(newState);
            }

            protected void updateRemaingDelays(ImmutableList<Duration> newRemainingRetrys) {
                remainingRetrysRef.set(newRemainingRetrys);
            }

            @Override
            public String toString() {
                return method + " " + target.toString();
            }
        }
    }

    
    
    
    static final class PersistentQueryQueue extends TransientQueryQueue {
        private final File dir;
        
        public PersistentQueryQueue(final Client client, 
                                    final URI target, 
                                    final Method method, 
                                    final int bufferSize, 
                                    final ImmutableSet<Integer> rejectStatusList,
                                    final ImmutableList<Duration> remainingRetries, 
                                    final int numParallelWorkers,
                                    final File dir) {
            
            super(client, target, method, bufferSize, rejectStatusList, remainingRetries, numParallelWorkers);
            this.dir = dir;
            
            // looks for old jobs and process it
            readUnlockedPersistentJobs().forEach(query -> tryReschedule(query));            
        }
        
        @Override
        public void close() {
            super.close();
            
            for (Query query : drainRunningQueries()) {
                ((PersistentQuery) query).close();
            }
        }
        
        private ImmutableList<PersistentQuery> readUnlockedPersistentJobs() {
            return ImmutableList.copyOf(dir.listFiles())
                                .stream()
                                .filter(file -> file.getName().endsWith(".query"))
                                .map(file -> PersistentQuery.tryReadFrom(PersistentQueryQueue.this, file))
                                .filter(optionalQuery -> optionalQuery.isPresent())
                                .map(optionalQuery -> optionalQuery.get())
                                .map(query -> { onQueryCreated(query); return query; })
                                .collect(Immutables.toList());
        }
        
        @Override
        public CompletableFuture<Submission> submitAsync(final Object entity, final String mediaType) {
            // Currently, persistent query supports json only
            if (!(mediaType.startsWith("application") && mediaType.endsWith("json"))) {
                throw new IllegalStateException("persistent retry supports JSON types only");
            }

            final Query query = newQuery(entity, mediaType);
            LOG.debug("submitting persistent query " + query.getId() + " " + query);

            return submitAsync(query);
        }
        
        private Query newQuery(final Object entity, final String mediaType) {
            final Query query = new PersistentQuery(target, method, Entity.entity(entity, mediaType), PersistentQueryQueue.this, remainingRetries, dir);
            onQueryCreated(query);
            return query;
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
                                    final QueryLifeCycleListener queryLifeCycleListener,
                                    final ImmutableList<Duration> remainingRetrys) throws IOException {
                super(id, target, method, entity, queryLifeCycleListener, remainingRetrys);
                
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
                                   final QueryLifeCycleListener queryLifeCycleListener,
                                   final ImmutableList<Duration> remainingRetrys, 
                                   final File dir) {
                super(target, method, entity, queryLifeCycleListener, remainingRetrys);
                
                /*
                 * By creating a new persistent query file a file lock will be acquired and not been released
                 * until query is completed! This avoids concurrent handling of the same queue (file).
                 */
                
                try {
                    queryFile = new File(dir.getCanonicalFile(), UUID.randomUUID().toString() + ".query");
                    raf = new RandomAccessFile(queryFile, "rw");
                    channel = raf.getChannel();
                    channel.lock();                           
                    writer = Channels.newWriter(channel, Charsets.UTF_8.toString());

                    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
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

                try {
                    MapEncoding.create()
                               .with("retries", Joiner.on("&").join(newRemaingDelays.stream().map(duration -> duration.toMillis()).collect(Immutables.toList()))) 
                               .writeTo(writer);
                } catch (RuntimeException rt) {
                    LOG.warn("could not update remaining delays of " + getId() + "  with " + newRemaingDelays, rt);
                }
                
                if (newRemaingDelays.isEmpty()) {
                    close();
                }
            }

            void close() {
                try {
                    if (channel.isOpen()) {
                        // by closing the file handles the lock will be released
                        Closeables.close(writer, true);
                        Closeables.close(channel, true);
                        Closeables.close(raf, true);

                        if (remainingRetrysRef.get().isEmpty()) {
                            queryFile.delete();
                        }
                    }
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            }

            public static Optional<PersistentQuery> tryReadFrom(final QueryLifeCycleListener queryLifeCycleListener, final File queryFile) {
                RandomAccessFile raf = null;
                FileChannel channel = null;
               
                /*
                 * By deserializing the underlying query file a file lock will be acquired 
                 * and not been released until query is completed!
                 * This avoids concurrent handling of the same queue (file).
                 */
                
                try {
                    raf = new RandomAccessFile(queryFile, "rw");
                    channel = raf.getChannel();
                    
                    // is already locked?
                    if (channel.tryLock() == null) {
                        LOG.debug("query file " + queryFile.getAbsolutePath() + " is locked. ignoring it");
                        raf.close();
                        return Optional.empty();
                        
                        
                    // no, go ahead by deserializing it
                    } else {
                        final LineNumberReader reader = new LineNumberReader(Channels.newReader(channel, Charsets.UTF_8.toString()));
                        final Writer writer = Channels.newWriter(channel, Charsets.UTF_8.toString());
                        
                        final MapEncoding protocol = MapEncoding.readFrom(reader);
                        final String id = protocol.get("id");
                        final Method method = protocol.get("method", txt -> Method.valueOf(txt));
                        final URI target = protocol.get("target", txt -> URI.create(txt));
                        final MediaType mediaType = protocol.get("mediaType", txt -> MediaType.valueOf(txt));
                        final byte[] data = protocol.get("data", txt -> Base64.getDecoder().decode(txt));
                        final ImmutableList<Duration> retries = protocol.get("retries", txt -> Strings.isNullOrEmpty(txt) ? ImmutableList.<Duration>of()  
                                                                                                                          : Splitter.on("&")
                                                                                                                                    .trimResults()
                                                                                                                                    .splitToList(txt)
                                                                                                                                    .stream()
                                                                                                                                    .map(milisString -> Duration.ofMillis(Long.parseLong(milisString)))
                                                                                                                                    .collect(Immutables.<Duration>toList()));        
                        // no retries left?
                        if (retries.isEmpty()) {
                            LOG.warn("deleting expired query file " + queryFile);
                            raf.close();
                            queryFile.delete();
                            return Optional.empty();
                        
                        // retries are available; return query (lock will not been released to avoid parallel processing of the query file) 
                        } else {
                            return Optional.of(new PersistentQuery(id,  
                                                                   queryFile,
                                                                   raf, 
                                                                   channel, 
                                                                   writer,
                                                                   target,
                                                                   method, 
                                                                   Entity.entity(data, mediaType),
                                                                   queryLifeCycleListener,
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