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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import net.oneandone.incubator.neo.http.sink.HttpSink.Method;



final class HttpSinkBuilderImpl implements HttpSinkBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(HttpSinkBuilderImpl.class);
    
    private final URI target;
    private final Client userClient;
    private final Method method;
    private final int bufferSize;
    private final File dir;
    private final ImmutableSet<Integer> rejectStatusList;
    private final ImmutableList<Duration> retryDelays;
    private final int numParallelWorkers;

    
    HttpSinkBuilderImpl(final Client userClient, 
                        final URI target, 
                        final Method method, 
                        final int bufferSize, 
                        final File dir, 
                        final ImmutableSet<Integer> rejectStatusList,
                        final ImmutableList<Duration> retryDelays, 
                        final int numParallelWorkers) {
        this.userClient = userClient;
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
        return new HttpSinkBuilderImpl(this.userClient, 
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
        return new HttpSinkBuilderImpl(this.userClient, 
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
        return new HttpSinkBuilderImpl(this.userClient,
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
        return new HttpSinkBuilderImpl(this.userClient, 
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
        return new HttpSinkBuilderImpl(this.userClient, 
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
        return new HttpSinkBuilderImpl(this.userClient, 
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
        return (dir == null) ? new TransientHttpSink() : new PersistentHttpSink(); 
    }   
    
    

    private class TransientHttpSink implements HttpSink {
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        private final Optional<Client> clientToClose;
        protected final Client httpClient;
        protected final ScheduledThreadPoolExecutor executor;
        protected final Monitor monitor = new Monitor();
    
    
        public TransientHttpSink() {
            this.executor = new ScheduledThreadPoolExecutor(numParallelWorkers);
    
            // using default client?
            if (userClient == null) {
                final Client defaultClient = ClientBuilder.newClient();
                this.httpClient = defaultClient;
                this.clientToClose = Optional.of(defaultClient);
                
            // .. no client is given by user 
            } else {
                this.httpClient = userClient;
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
                for (TransientSubmission query : monitor.getAll()) {
                    query.release();
                }
                
                clientToClose.ifPresent(c -> c.close());
                executor.shutdown();
            }
        }
        
        @Override
        public Metrics getMetrics() {
            return monitor;
        }
        
        @Override
        public CompletableFuture<Submission> submitAsync(final Object entity, final String mediaType) {
            try {
                final TransientSubmission query = newQuery(Entity.entity(entity, mediaType), UUID.randomUUID().toString());
        
                LOG.debug("submitting " + query);
                return query.process().thenApply(q -> (Submission) q);
            } catch (IOException ioe) {
                final CompletableFuture<Submission> promise = new CompletableFuture<>();
                promise.completeExceptionally(ioe);
                return promise;
            }
        }
    
        protected TransientSubmission newQuery(final Entity<?> entity, final String id) throws IOException {
            return new TransientSubmission(httpClient, 
                                           id, 
                                           target, 
                                           method, 
                                           entity,
                                           rejectStatusList,
                                           retryDelays,
                                           0,
                                           executor,
                                           monitor);
        }
        
        @Override
        public String toString() {
            return method + " " + target + "\r\n" + getMetrics();
        }
    }
    
    
    private final class PersistentHttpSink extends TransientHttpSink {

        public PersistentHttpSink() {
            super();
            PersistentSubmission.processOldQueryFiles(dir, method, target, httpClient, executor, monitor);
        }

        @Override
        protected TransientSubmission newQuery(final Entity<?> entity, final String id) throws IOException {
            return new PersistentSubmission(httpClient,
                                            id, 
                                            target, 
                                            method,   
                                            entity,
                                            rejectStatusList,
                                            retryDelays,
                                            0,
                                            executor,  
                                            monitor,
                                            dir);
        }    
    } 
}