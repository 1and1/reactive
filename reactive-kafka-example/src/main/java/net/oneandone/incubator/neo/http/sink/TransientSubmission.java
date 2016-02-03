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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.ResponseProcessingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import net.oneandone.incubator.neo.exception.Exceptions;
import net.oneandone.incubator.neo.http.sink.HttpSink.Method;
import net.oneandone.incubator.neo.http.sink.HttpSink.Submission;



class TransientSubmission implements Submission {
    private static final Logger LOG = LoggerFactory.getLogger(TransientSubmission.class);

    // state
    private final AtomicReference<HttpSink.Submission.State> stateRef = new AtomicReference<>(HttpSink.Submission.State.PENDING);
    private final AtomicInteger numRetries;
    
    private final String id;
    private final URI target;
    private final Entity<?> entity;
    private final ImmutableSet<Integer> rejectStatusList;
    private final ScheduledThreadPoolExecutor executor;
    private final ImmutableList<Duration> retryDelays;
    protected final Method method;
    protected final Client client;
    protected final Monitor monitor;
  
    
    protected TransientSubmission(final Client client,
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
    
    protected CompletableFuture<TransientSubmission> process() {
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

    
    private final class ResponseHandler extends CompletableFuture<TransientSubmission> implements InvocationCallback<String> {
     
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
                LOG.warn("rejecting query " + TransientSubmission.this);

                monitor.incReject();
                terminate();
                
                completeExceptionally(error);
                
            // ..no     
            } else {

                // retries left?
                boolean isRescheduled = tryReschedule();
                if (isRescheduled) {
                    complete(TransientSubmission.this);
                    
                // .. no
                } else {
                    completeExceptionally(error);
                }
            }
        }

        @Override
        public void completed(String responseEntity) {
            LOG.debug("query " + getId() + " executed successfully");
            TransientSubmission.this.completed();
            monitor.incSuccess();

            complete(TransientSubmission.this);
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