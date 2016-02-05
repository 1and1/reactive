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
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.ResponseProcessingException;
import javax.ws.rs.client.Invocation.Builder;

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

    private final String id;
    private final URI target;
    private final Entity<?> entity;
    private final ImmutableSet<Integer> rejectStatusList;
    private final ImmutableList<Duration> processDelays;
    protected final Method method;
  
    // state
    protected final AtomicReference<SubmissionState> stateRef;

    
    protected TransientSubmission(final String id,
                                  final URI target, 
                                  final Method method,    
                                  final Entity<?> entity,
                                  final ImmutableSet<Integer> rejectStatusList,
                                  final ImmutableList<Duration> processDelays,
                                  final int numRetries,
                                  final Instant dateLastTrial) {
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(target);
        Preconditions.checkNotNull(method);
        Preconditions.checkNotNull(entity);
        
        this.id = id;
        this.target = target;
        this.method = method;
        this.entity = entity;
        this.processDelays = processDelays;
        this.rejectStatusList = rejectStatusList;
        
        stateRef = new AtomicReference<>(new PendingState(numRetries, dateLastTrial));
    }
    
    public String getId() {
        return id;
    }

    @Override
    public State getState() {
        return stateRef.get().getState();
    }
    
    public CompletableFuture<Boolean> process(final Client httpClient, final ScheduledThreadPoolExecutor executor) {
        return stateRef.get().process(httpClient, executor);
    }
   
    protected void updateState(FinalState newState) {
        stateRef.set(newState);
    }
    
    protected void updateState(PendingState newState) {
        stateRef.set(newState);
    }
    
    protected void release() {
    }             

    @Override
    public String toString() {
        return getId() + " - " + method + " " + target + " (" + stateRef.get() + ")";
    }
 
   
    protected abstract class SubmissionState {
        private final State state;
        
        public SubmissionState(State state) {
            this.state = state;      
        }
        
        State getState() {
            return state;
        }
        
        CompletableFuture<Boolean> process(final Client httpClient, final ScheduledThreadPoolExecutor executor) {
            throw new IllegalStateException("can not process submission. State is " + getState());
        }
        
        
        @Override
        public String toString() {
            return "state=" + state.toString();
        }
    }
    
    protected class FinalState extends SubmissionState {
        
        public FinalState(State state) {
            super(state);
        }
    }
     
    
    protected final class PendingState extends SubmissionState {
        private final int numTrials;
        private final Instant dateLastTrial;
        
        public PendingState(int numTrials, Instant dateLastTrial) {
            super(State.PENDING);
            this.numTrials = numTrials;
            this.dateLastTrial = dateLastTrial;
        }
        
        public int getNumTrials() {
            return numTrials;
        }
        
        public Instant getDateLastTrial() {
            return dateLastTrial;
        }
        
        private Optional<Duration> nextExecutionDelay(int distance) {
            final int pos = numTrials + distance; 
            if (pos < processDelays.size()) {
                return Optional.of(processDelays.get(pos));
            } else {
                return Optional.empty();
            }
        }
                
        @Override
        public CompletableFuture<Boolean> process(final Client httpClient, final ScheduledThreadPoolExecutor executor) {
            // trials left?
            final Optional<Duration> optionalDelay = nextExecutionDelay(0); 
            if (optionalDelay.isPresent()) {
                final Duration elapsedSinceLastRetry = Duration.between(dateLastTrial, Instant.now());
                final Duration correctedDelay = optionalDelay.get().minus(elapsedSinceLastRetry);
                final Duration delay = correctedDelay.isNegative() ? Duration.ZERO : correctedDelay;
                
                SubmitTask task = new SubmitTask(httpClient);
                executor.schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS);
                return task;
            } else {
                throw new RuntimeException("no trials left");
            }
        }
        
        private class SubmitTask extends CompletableFuture<Boolean> implements InvocationCallback<String>, Runnable {
            private final Client httpClient;
            private final int run;
            
            public SubmitTask(final Client httpClient) {
                this.httpClient = httpClient;
                this.run = numTrials + 1;
            }
            
            @Override
            public void run() {
                // initiate http request
                LOG.debug("performing submission " + getId() + " (" + (run) + " of " + processDelays.size() + ")");

                // http request is going to be initiated -> update state with incremented trials
                updateState(new PendingState(run, Instant.now()));
                
                try {
                    final Builder builder = httpClient.target(target).request();
                    if (method == Method.POST) {
                        builder.async().post(entity, this);
                    } else {
                        builder.async().put(entity, this);
                    }
                    
                } catch (RuntimeException rt) {
                    this.failed(rt);
                }
            }
            
            @Override
            public void failed(final Throwable ex) {
                final Throwable error;
                if (ex instanceof ResponseProcessingException) {
                    error = ((ResponseProcessingException) ex).getCause();
                } else {
                    error = ex;
                }

                // rejecting error?
                int status = readAssociatedStatus(error);
                if (rejectStatusList.contains(status)) {
                    LOG.debug("submission " + getId() + " failed with rejecting status " + status + " Discarding submission");
                    updateState(new FinalState(State.DISCARDED));
                    completeExceptionally(error);
                    
                // ..no     
                } else {
                    // retries left?
                    Optional<Duration> nextRetry = nextExecutionDelay(1);
                    if (nextRetry.isPresent()) {
                        LOG.debug("submission " + getId() + " failed with "  + (processDelays.size() - run) + " retrys left (next in " + nextRetry.get() + ")");
                        complete(false);
                    } else {
                        LOG.debug("submission " + getId() + " failed with no retrys left.  Discarding submission");
                        updateState(new FinalState(State.DISCARDED));
                        completeExceptionally(error);
                    }
                }
            }

            @Override
            public void completed(String responseEntity) {
                LOG.debug("query " + getId() + " executed successfully");
                updateState(new FinalState(State.COMPLETED));
                complete(true);
            }
        }
        
        private int readAssociatedStatus(final Throwable t) { 
            final Throwable error = Exceptions.unwrap(t);
            return (error instanceof WebApplicationException) ? ((WebApplicationException) error).getResponse().getStatus() 
                                                              : 500;
        }
        
        @Override
        public String toString() {
            return super.toString() +  " : " + processDelays.subList(numTrials, processDelays.size()) + " trials left of " + processDelays;
        }
    }
}