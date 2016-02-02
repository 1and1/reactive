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

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.ResponseProcessingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import net.oneandone.incubator.neo.exception.Exceptions;



class TransientHttpSink implements HttpSink {
    private static final Logger LOG = LoggerFactory.getLogger(TransientHttpSink.class);
    
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final Optional<Client> clientToClose;
    protected final ImmutableSet<Integer> rejectStatusList;
    protected final Client client;
    protected final ScheduledThreadPoolExecutor executor;
    protected final URI target;
    protected final Method method;
    protected final ImmutableList<Duration> retryDelays;
    protected final Monitor monitor = new Monitor();


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
        final Query query = new Query(client, 
                                      UUID.randomUUID().toString(), 
                                      target, 
                                      method, 
                                      Entity.entity(entity, mediaType),
                                      rejectStatusList,
                                      retryDelays,
                                      0,
                                      executor,
                                      monitor);

        LOG.debug("submitting " + query);
        return query.process().thenApply(q -> (Submission) q);
    }
    
    
    @Override
    public String toString() {
        return method + " " + target + "\r\n" + getMetrics();
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
                        final ImmutableSet<Integer> rejectStatusList,
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
    
    
   
    
    protected static final class Monitor implements Metrics {
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
        
        public void onAcquired(Query query) {
            synchronized (this) {
                runningQuery.add(query);
            }
        }
        
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
}