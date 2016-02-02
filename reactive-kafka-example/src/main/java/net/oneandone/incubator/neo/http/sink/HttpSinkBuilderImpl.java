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
import java.util.concurrent.atomic.AtomicInteger;
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
    private final ImmutableList<Duration> retryDelays;
    private final int numParallelWorkers;

    
    HttpSinkBuilderImpl(final Client client, 
                        final URI target, 
                        final Method method, 
                        final int bufferSize, 
                        final File dir, 
                        final ImmutableSet<Integer> rejectStatusList,
                        final ImmutableList<Duration> retryDelays, 
                        final int numParallelWorkers) {
        this.client = client;
        this.target = target;
        this.method = method;
        this.bufferSize = bufferSize;
        this.dir = dir;
        this.rejectStatusList = rejectStatusList;
        this.retryDelays = retryDelays;
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
                                       this.retryDelays, 
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
                                       this.retryDelays,
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
                                       this.retryDelays,
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
                                       this.retryDelays,
                                       this.numParallelWorkers);
    }

    @Override
    public HttpSinkBuilder withPersistency(final File dir) {
        Preconditions.checkNotNull(dir);
        return new HttpSinkBuilderImpl(this.client, 
                                       this.target, 
                                       this.method, 
                                       this.bufferSize, 
                                       dir,
                                       this.rejectStatusList,
                                       this.retryDelays, 
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
                                       this.retryDelays, 
                                       this.numParallelWorkers);
    }

    
    /**
     * @return the sink reference
     */
    public HttpSink open() {

        // if dir is set, sink will run in persistent mode  
        return (dir == null) ? new TransientHttpSink(client,                 // queries will be stored in-memory only      
                                                     target, 
                                                     method, 
                                                     bufferSize, 
                                                     rejectStatusList, 
                                                     retryDelays,
                                                     numParallelWorkers)               
                             : new PersistentHttpSink(client,                // queries will be stored on file system as well
                                                      target, 
                                                      method,
                                                      bufferSize, 
                                                      rejectStatusList,
                                                      retryDelays,
                                                      numParallelWorkers, 
                                                      dir);    
    }


    
    static class TransientHttpSink implements HttpSink {
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        private final ImmutableSet<Integer> rejectStatusList;
        private final Optional<Client> clientToClose;
        private final Client client;
        private final ScheduledThreadPoolExecutor executor;
        private final URI target;
        private final Method method;
        private final ImmutableList<Duration> retryDelays;
        private final Monitor monitor = new Monitor();


        public TransientHttpSink(final Client client, 
                                 final URI target, 
                                 final Method method, 
                                 final int bufferSize, 
                                 final ImmutableSet<Integer> rejectStatusList,
                                 final ImmutableList<Duration> retryDelays, 
                                 final int numParallelWorkers) {
            this.target = target;
            this.method = method;
            this.rejectStatusList = rejectStatusList;
            this.retryDelays = retryDelays;
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
                for (Query query : monitor.getAll()) {
                    query.release();
                }
                
                clientToClose.ifPresent(client -> client.close());
                executor.shutdown();
            }
        }
        
        @Override
        public Metrics getMetrics() {
            return monitor;
        }
        
        @Override
        public CompletableFuture<Submission> submitAsync(Object entity, String mediaType) {
            return Query.openAsync(client, 
                                   target, 
                                   method, 
                                   Entity.entity(entity, mediaType),
                                   rejectStatusList,
                                   retryDelays,
                                   executor,
                                   monitor)
                        .thenApply(query -> (Submission) query);
        }
        
        
        @Override
        public String toString() {
            return method + " " + target + "\r\n" + getMetrics();
        }
 
        
        static final class Monitor implements Metrics, QueryLifeClyceListener {
            private final Set<Query> runningQuery = Collections.newSetFromMap(new WeakHashMap<Query, Boolean>());
            private final MetricRegistry metrics = new MetricRegistry();
            private final Counter success = metrics.counter("success");
            private final Counter retries = metrics.counter("retries");
            private final Counter discarded = metrics.counter("discarded");
            private final Counter rejected = metrics.counter("rejected");

            public void incSuccess() {
                success.inc();
            }
            
            @Override
            public Counter getNumSuccess() {
                return success;
            }
            
            public void incRetry() {
                retries.inc();
            }

            @Override
            public Counter getNumRetries() {
                return retries;
            }
            
            public void incReject() {
                rejected.inc();
            }

            @Override
            public Counter getNumRejected() {
                return rejected;
            }

            public void incDiscard() {
                discarded.inc();
            }
            
            @Override
            public Counter getNumDiscarded() {
                return discarded;
            }
            
            @Override
            public int getNumPending() {
                return getAll().size();
            }
            
            
            @Override
            public void onAcquired(Query query) {
                synchronized (this) {
                    runningQuery.add(query);
                }
            }
            
            @Override
            public void onReleased(Query query) {
                synchronized (this) {
                    runningQuery.remove(query);
                }
            }

            public ImmutableSet<Query> getAll() {
                ImmutableSet<Query> runnings;
                synchronized (this) {
                    runnings = ImmutableSet.copyOf(runningQuery);
                }
                return runnings;
            }
            
            @Override
            public String toString() {
                return new StringBuilder().append("panding=" + getNumPending())
                                          .append("success=" + getNumSuccess().getCount())
                                          .append("retries=" + getNumRetries().getCount())
                                          .append("discarded=" + getNumDiscarded().getCount())
                                          .append("rejected=" + getNumRejected().getCount())
                                          .toString();
            }
        }
                

        public static interface QueryLifeClyceListener {
                
            void onAcquired(Query query);
            
            void onReleased(Query query);
        }

        
        protected static class Query implements Submission {
            protected final Client client;
            private final String id;
            protected final Method method;
            private final URI target;
            private final Entity<?> entity;
            private final AtomicReference<HttpSink.Submission.State> stateRef = new AtomicReference<>(HttpSink.Submission.State.PENDING);
            private final ImmutableSet<Integer> rejectStatusList;
            private final ScheduledThreadPoolExecutor executor;
            private final ImmutableList<Duration> retryDelays;
            private final AtomicInteger numRetries;
            
            protected final Monitor monitor;
          
            
            protected Query(final Client client,
                            final String id,
                            final URI target, 
                            final Method method,    
                            final Entity<?> entity,
                            ImmutableSet<Integer> rejectStatusList,
                            final ImmutableList<Duration> retryDelays,
                            final int numRetries,
                            final ScheduledThreadPoolExecutor executor,
                            final Monitor monitor) {
                Preconditions.checkNotNull(id);
                Preconditions.checkNotNull(target);
                Preconditions.checkNotNull(method);
                Preconditions.checkNotNull(entity);
                
                this.client = client;
                this.id = id;
                this.target = target;
                this.method = method;
                this.entity = entity;
                this.retryDelays = retryDelays;
                this.numRetries = new AtomicInteger(numRetries); 
                this.rejectStatusList = rejectStatusList;
                this.executor = executor;
                this.monitor = monitor;
                
                monitor.onAcquired(this);
            }

            public static CompletableFuture<Query> openAsync(final Client client,
                                                             final URI target, 
                                                             final Method method,    
                                                             final Entity<?> entity,
                                                             final ImmutableSet<Integer> rejectStatusList,
                                                             final ImmutableList<Duration> remainingRetrys,
                                                             final ScheduledThreadPoolExecutor executor,
                                                             final Monitor monitor) {
                final Query query = new Query(client, 
                                              UUID.randomUUID().toString(), 
                                              target, 
                                              method, 
                                              entity,
                                              rejectStatusList,
                                              remainingRetrys,
                                              0,
                                              executor,
                                              monitor);
                
                LOG.debug("submitting " + query);
                return query.process();
            }
            
            public String getId() {
                return id;
            }

            @Override
            public State getState() {
                return stateRef.get();
            }
            
            private void updateState(State newState) {
                stateRef.set(newState);
                
                if ((newState == State.COMPLETED) || (newState == State.DISCARDED)) {
                    setNumRetries(Integer.MAX_VALUE);
                }
            }

            protected void setNumRetries(int num) {
                numRetries.set(num);
                
                if (!isRetriesLeft()) {
                    release();
                }
            }
            
            protected boolean isRetriesLeft() {
                return (numRetries.get() < retryDelays.size());
            }
            
            public void terminate() {
                updateState(State.DISCARDED);
            }

            private void completed() {
                updateState(State.COMPLETED);
            }

            protected void release() {
                monitor.onReleased(this);
            }             
            
            @Override
            public String toString() {
                return getId() + " - " + method + " " + target;
            }
            
            protected CompletableFuture<Query> process() {
                final ResponseHandler responseHandler = new ResponseHandler();

                if (getState() == State.PENDING) {
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
                    
                } else {
                    responseHandler.failed(new IllegalStateException("query in state " + getState()));
                }
                
                return responseHandler;
            }

            private final class ResponseHandler extends CompletableFuture<Query> implements InvocationCallback<String> {
             
                @Override
                public void failed(final Throwable ex) {
                    final Throwable error;
                    if (ex instanceof ResponseProcessingException) {
                        error = ((ResponseProcessingException) ex).getCause();
                    } else {
                        error = ex;
                    }

                    LOG.debug("query " + getId() + " failed.", error);
                    
                    // rejecting error?
                    if (rejectStatusList.contains(readAssociatedStatus(error))) {
                        LOG.warn("rejecting query " + Query.this);

                        monitor.incReject();
                        terminate();
                        
                        completeExceptionally(error);
                        
                    // ..no     
                    } else {

                        // retries left?
                        boolean isRescheduled = tryReschedule();
                        if (isRescheduled) {
                            complete(Query.this);
                            
                        // .. no
                        } else {
                            completeExceptionally(error);
                        }
                    }
                }

                @Override
                public void completed(String responseEntity) {
                    LOG.debug("query " + getId() + " executed successfully");
                    Query.this.completed();
                    monitor.incSuccess();

                    complete(Query.this);
                }
            }
            
            private static int readAssociatedStatus(final Throwable t) { 
                final Throwable error = Exceptions.unwrap(t);
                return (error instanceof WebApplicationException) ? ((WebApplicationException) error).getResponse().getStatus() 
                                                                  : 500;
            }
            
            
            protected boolean tryReschedule() {
                
                if (isRetriesLeft()) {
                    final int pos = numRetries.get();
                    final Duration delay = retryDelays.get(pos);
                    LOG.debug("enqueue query " + getId() + " for retry (" + (pos + 1) + " of " + retryDelays.size() + ") after " + delay);

                    final Runnable delayedTask = () -> {
                                                        LOG.debug("performing retry (" + (pos + 1) + " of " + retryDelays.size() + ") query  " + getId());
                                                        monitor.incRetry();                            
                                                        setNumRetries(pos + 1); 
                                                        process();
                    };
                    executor.schedule(delayedTask, delay.toMillis(), TimeUnit.MILLISECONDS);          
                    return true;
                    
                } else {
                    LOG.debug("no retries left " + getId());
                    monitor.incDiscard();
                    terminate();

                    return false;
                }
            }
        }
    }



    
    static final class PersistentHttpSink extends TransientHttpSink {
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
    
   
}