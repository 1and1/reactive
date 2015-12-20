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

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.ResponseProcessingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;




public class HttpSink {
    private static final Logger LOG = LoggerFactory.getLogger(HttpSink.class);

    public enum Method { POST, PUT} ;
    
    private final URI target;
    private final Client client;
    private final Method method;
    private final int bufferSize;
    private final File dir;
    private final Optional<String> appId;
    private final ImmutableList<Duration> remainingRetries;
    private final int numParallelWorkers;
    
    
    private HttpSink(Client client, 
                     URI target, 
                     Method method, 
                     int bufferSize, 
                     File dir, 
                     Optional<String> appId,
                     ImmutableList<Duration> remainingRetries,
                     int numParallelWorkers) {
        this.client = client;
        this.target = target;
        this.method = method;
        this.bufferSize = bufferSize;
        this.dir = dir;
        this.appId = appId;
        this.remainingRetries = remainingRetries;
        this.numParallelWorkers = numParallelWorkers;
    }
    
    
    public static HttpSink create(String target) {
        return create(URI.create(target));
    }
    
    public static HttpSink create(URI target) {
        return new HttpSink(null, target, Method.POST, Integer.MAX_VALUE, null, Optional.empty(), ImmutableList.of(), 1);
    }
    
    public HttpSink withClient(Client client) {
        return new HttpSink(client, 
                            this.target, 
                            this.method, 
                            this.bufferSize, 
                            this.dir, 
                            this.appId, 
                            this.remainingRetries, 
                            this.numParallelWorkers);
    }
    
    public HttpSink withMethod(Method method) {
        return new HttpSink(this.client, 
                            this.target, method, 
                            this.bufferSize,
                            this.dir, 
                            this.appId, 
                            this.remainingRetries, 
                            this.numParallelWorkers);
    }

    public HttpSink withRetryAfter(ImmutableList<Duration> retryPauses) {
        return new HttpSink(this.client,
                            this.target, 
                            this.method, 
                            this.bufferSize,
                            this.dir,
                            this.appId,
                            retryPauses,
                            this.numParallelWorkers);
    }
    
    public HttpSink withRetryParallelity(int numParallelWorkers) {
        return new HttpSink(this.client, 
                            this.target, 
                            this.method,
                            this.bufferSize,
                            this.dir, 
                            this.appId,
                            this.remainingRetries, 
                            numParallelWorkers);
    }
    
    public HttpSink withRetryBufferSize(int bufferSize) {
        return new HttpSink(this.client, 
                            this.target, 
                            this.method, 
                            bufferSize,
                            this.dir, 
                            this.appId, 
                            this.remainingRetries,
                            this.numParallelWorkers);
    }
    
    public HttpSink withRetryPersistency(File dir) {
        return new HttpSink(this.client, 
                            this.target, 
                            this.method, 
                            this.bufferSize, 
                            dir, 
                            this.appId, 
                            this.remainingRetries,
                            this.numParallelWorkers);
    }

    public EntityConsumer open() {
        return (dir == null) ? new InMemoryQueryQueue() : new FileQueryQueue(); 
    }
    
    
    
    private abstract class QueryQueue implements EntityConsumer {

        private final AtomicReference<Client> defaultClientRef = new AtomicReference<>(); 
        
        // statistics
        private final AtomicLong success = new AtomicLong(0);
        private final AtomicLong retries = new AtomicLong(0);
        private final AtomicLong discarded = new AtomicLong(0);

        @Override
        public long getNumSuccess() {
            return success.get();
        }

        @Override
        public long getNumRetries() {
            return retries.get();
        }

        @Override
        public long getNumDiscarded() {
            return discarded.get();
        }
        
        @Override
        public void close() {
            Client defaultClient = defaultClientRef.get();
            if (defaultClient != null) {
                defaultClient.close();
            }
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
        public CompletableFuture<Boolean> acceptAsync(Object entity, String mediaType) {
            final CompletableFuture<Boolean> promise = new CompletableFuture<>();
            final Query query = new Query(target, method, appId, Entity.entity(entity, mediaType), Duration.ZERO, remainingRetries);
            
            execute(query, promise);
            
            return promise;
        }
                
        private void execute(Query query, CompletableFuture<Boolean> promise) {
            query.executeWith(getClient())
                 .whenComplete((Void, error) -> {
                                                     try {
                                                         
                                                         if (error == null) {
                                                             success.incrementAndGet();
                                                             promise.complete(true);
                                                             
                                                         } else if (error instanceof ClientErrorException) {
                                                             LOG.debug("got client error complete exceptionally", error);
                                                             promise.completeExceptionally(error);
                                                             
                                                         } else {
                                                             LOG.warn("query failed: " + query);
                                                             if (query.getRetryQuery().isPresent()) {
                                                                 enqueue( query.getRetryQuery().get());
                                                             } else {
                                                                 discarded.incrementAndGet();
                                                                 LOG.warn("no retries left! discard query " + query);
                                                             }
                                                             promise.complete(false);
                                                         }
                                                         
                                                     } catch (RuntimeException rt) {
                                                         promise.completeExceptionally(rt);
                                                     }
                                                });
        }

        protected void executeRetry(Query query) {
            retries.incrementAndGet();
            execute(query, new CompletableFuture<>());
        }
        
        protected abstract void enqueue(Query query);
    }

    


    
    private class InMemoryQueryQueue extends QueryQueue {
        private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(numParallelWorkers); 
        
        @Override
        protected void enqueue(Query query) {
            LOG.warn("schedule retry query " + query + " after " + query.getDelayBeforeExecute());
            executor.schedule(() -> executeRetry(query), 
                              query.getDelayBeforeExecute().toMillis(), 
                              TimeUnit.MILLISECONDS);
        }
        
        @Override
        public int getQueueSize() {
            return executor.getActiveCount();
        }
        
        @Override
        public void close() {
            executor.shutdown();
            super.close();
        }
    }

    
    
    
    private class FileQueryQueue extends QueryQueue {

        @Override
        protected void enqueue(Query query) {
            
        }

        @Override
        public int getQueueSize() {
            return 0;
        }
        
        @Override
        public void close() {
            super.close();
        }
    }

    
    
    private static final class Query {
        private final Method method;
        private final URI target;
        private final Optional<String> appId;
        private final Entity<?> entity;
        private final Duration delayBeforeExecution;
        private final ImmutableList<Duration> remainingRetrys;
        
        public Query(final URI target,
                     final Method method, 
                     final Optional<String> appId, Entity<?> entity,
                     final Duration delayBeforeExecution,
                     final ImmutableList<Duration> remainingRetrys) {
            this.target = target;
            this.method = method;
            this.appId = appId;
            this.entity = entity;
            this.delayBeforeExecution = delayBeforeExecution;
            this.remainingRetrys = remainingRetrys;
        }
        
        public Optional<Query> getRetryQuery() {
            if (remainingRetrys.isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(new Query(target, method, appId, entity, remainingRetrys.get(0), remainingRetrys.subList(1, remainingRetrys.size())));
            }
        }
        
        public Duration getDelayBeforeExecute() {
            return delayBeforeExecution;
        }
        
        public CompletableFuture<Void> executeWith(Client client) {
            final ResponseHandler responseHandler = new ResponseHandler();
            
            final Builder builder = client.target(target).request();
            appId.ifPresent(id -> builder.header("X-APP", id));
            
            if (method == Method.POST) {
                builder.async().post(entity, responseHandler);
            } else {
                builder.async().put(entity, responseHandler);
            }
            
            return responseHandler;
        }
        
        
        @Override
        public String toString() {
            return method + " " + target.toString();
        }
        
        private static final class ResponseHandler extends CompletableFuture<Void> implements InvocationCallback<String> {
            
            @Override
            public void failed(Throwable ex) {
                if (ex instanceof ResponseProcessingException) {
                    ex = ((ResponseProcessingException) ex).getCause();
                }
                completeExceptionally(ex);
            }
            
            @Override
            public void completed(String responseEntity) {
                complete(null);
            }
        }
    }
}
