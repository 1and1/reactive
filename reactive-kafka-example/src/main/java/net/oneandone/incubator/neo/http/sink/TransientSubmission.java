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

    protected final String id;
    protected final URI target;
    protected final Entity<?> entity;
    protected final ImmutableSet<Integer> rejectStatusList;
    protected final ImmutableList<Duration> processDelays;
    protected final Method method;
  
    // submission state
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
   
    public CompletableFuture<Boolean> processAsync(final Client httpClient, final ScheduledThreadPoolExecutor executor) {
        return stateRef.get().processAsync(httpClient, executor);
    }
   
    protected CompletableFuture<Void> updateStateAsync(final FinalState newState) {
        stateRef.set(newState);
        return CompletableFuture.completedFuture(null);
    }
    
    protected CompletableFuture<Void> updateStateAsync(final PendingState newState) {
        stateRef.set(newState);
        return CompletableFuture.completedFuture(null);
    }
    
    protected void release() {
    }             

    @Override
    public String toString() {
        return getId() + " - " + method + " " + target + " (" + stateRef.get() + ")";
    }
 
   
    protected abstract class SubmissionState {
        private final State state;
        protected final int numTrials;
        protected final Instant dateLastTrial;

        public SubmissionState(final State state, final int numTrials, final Instant dateLastTrial) {
            this.state = state;      
            this.numTrials = numTrials;
            this.dateLastTrial = dateLastTrial;
        }
        
        State getState() {
            return state;
        }
        
        int getNumTrials() {
            return numTrials;
        }
        
        Instant getDateLastTrial() {
            return dateLastTrial;
        }

        CompletableFuture<Boolean> processAsync(final Client httpClient, final ScheduledThreadPoolExecutor executor) {
            throw new IllegalStateException("can not process submission. State is " + getState());
        }
                
        @Override
        public String toString() {
            return "state=" + state.toString();
        }
    }
    
    protected class FinalState extends SubmissionState {
        
        public FinalState(final State state, final int numTrials, final Instant dateLastTrial) {
            super(state, numTrials, dateLastTrial);
        }
    }
     
    
    protected final class PendingState extends SubmissionState {
        
        public PendingState(final int numTrials, final Instant dateLastTrial) {
            super(State.PENDING, numTrials, dateLastTrial);
        }
        
        private Optional<Duration> nextExecutionDelay(final int distance) {
            final int pos = numTrials + distance; 
            if (pos < processDelays.size()) {
                return Optional.of(processDelays.get(pos));
            } else {
                return Optional.empty();
            }
        }
                
        @Override
        public CompletableFuture<Boolean> processAsync(final Client httpClient, final ScheduledThreadPoolExecutor executor) {
            // trials left?
            final Optional<Duration> optionalDelay = nextExecutionDelay(0); 
            if (optionalDelay.isPresent()) {
                final Duration elapsedSinceLastRetry = Duration.between(dateLastTrial, Instant.now());
                final Duration correctedDelay = optionalDelay.get().minus(elapsedSinceLastRetry);
                final Duration delay = correctedDelay.isNegative() ? Duration.ZERO : correctedDelay;
                
                final SubmitTask task = new SubmitTask(httpClient);
                executor.schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS);
                return task;
            } else {
                return Exceptions.completedFailedFuture(new RuntimeException("no trials left"));
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
                try {
                    LOG.debug("performing submission " + getId() + " (" + (run) + " of " + processDelays.size() + ")");
                    TransientSubmission.this.updateStateAsync(new PendingState(run, Instant.now()))     // update state with incremented trials
                                            .thenAccept((Void) -> performHttpQuery());                  // initiate http query
                } catch (RuntimeException rt) {
                    failed(rt);
                }
            }
            
            private void performHttpQuery() {
                try {
                    final Builder builder = httpClient.target(target).request();
                    if (method == Method.POST) {
                        builder.async().post(entity, this);  // this -> submit task implements invocation listener interface
                    } else {
                        builder.async().put(entity, this);   // this -> submit task implements invocation listener interface
                    }
                } catch (RuntimeException rt) {
                    failed(rt);
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
                final int status = readAssociatedStatus(error);
                if (rejectStatusList.contains(status)) {
                    LOG.debug("submission " + getId() + " failed with rejecting status " + status + " Discarding submission");
                    updateStateAsync(new FinalState(State.DISCARDED, numTrials, dateLastTrial))
                                 .thenAccept((Void) -> completeExceptionally(error));
                    
                // ..no     
                } else {
                    // retries left?
                    final Optional<Duration> nextRetry = nextExecutionDelay(1);
                    if (nextRetry.isPresent()) {
                        LOG.debug("submission " + getId() + " failed with "  + (processDelays.size() - run) + " retrys left (next in " + nextRetry.get() + ")");
                        complete(false);
                    } else {
                        LOG.debug("submission " + getId() + " failed with no retrys left.  Discarding submission");
                        updateStateAsync(new FinalState(State.DISCARDED, numTrials, dateLastTrial))
                                     .thenAccept((Void) -> completeExceptionally(error));
                    }
                }
            }

            @Override
            public void completed(String responseEntity) {
                LOG.debug("query " + getId() + " executed successfully");
                updateStateAsync(new FinalState(State.COMPLETED, numTrials, dateLastTrial))
                             .thenAccept((Void) -> complete(true));
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