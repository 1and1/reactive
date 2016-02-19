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
import javax.ws.rs.client.Invocation.Builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import net.oneandone.incubator.neo.exception.Exceptions;
import net.oneandone.incubator.neo.http.sink.HttpSink.Method;
import net.oneandone.incubator.neo.http.sink.HttpSink.Submission;
import net.oneandone.incubator.neo.http.sink.HttpSink.Submission.State;


class TransientSubmissionTask {
    private static final Logger LOG = LoggerFactory.getLogger(TransientSubmissionTask.class);

    private final QueryExecutor queryExecutor = new QueryExecutor();
    
    protected final String id;
    protected final URI target;
    protected final Entity<?> entity;
    protected final ImmutableSet<Integer> rejectStatusList;
    protected final ImmutableList<Duration> processDelays;
    protected final Method method;
    protected final int numTrials;
    protected final Instant dateLastTrial;
    protected final SubmissionImpl submission;
    private final Duration nextExecutionDelay;
    
   
    protected TransientSubmissionTask(final String id,
                                      final URI target, 
                                      final Method method,    
                                      final Entity<?> entity,
                                      final ImmutableSet<Integer> rejectStatusList,
                                      final ImmutableList<Duration> processDelays,
                                      final int numTrials,
                                      final Instant dateLastTrial,
                                      final SubmissionImpl submission) {
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
        this.numTrials = numTrials;
        this.dateLastTrial = dateLastTrial;
        this.submission = submission;
        
        Duration nextDelay = processDelays.get(numTrials); 
        final Duration elapsedSinceLastRetry = Duration.between(dateLastTrial, Instant.now());
        final Duration correctedDelay = nextDelay.minus(elapsedSinceLastRetry);
        this.nextExecutionDelay = correctedDelay.isNegative() ? Duration.ZERO : correctedDelay;
    }
    
    public static CompletableFuture<TransientSubmissionTask> newPersistentSubmissionAsync(final String id,
                                                                                          final URI target, 
                                                                                          final Method method,    
                                                                                          final Entity<?> entity,
                                                                                          final ImmutableSet<Integer> rejectStatusList,   
                                                                                          final ImmutableList<Duration> processDelays,
                                                                                          final int numTrials,
                                                                                          final Instant dateLastTrial) {

        return CompletableFuture.completedFuture(new TransientSubmissionTask(id,
                                                                             target, 
                                                                             method, 
                                                                             entity,
                                                                             rejectStatusList,
                                                                             processDelays,
                                                                             numTrials,
                                                                             dateLastTrial,
                                                                             new SubmissionImpl()));   
    }
        
    protected TransientSubmissionTask cloneSubmissionTask(final int numTrials,
                                                          final Instant dateLastTrial) {
        return new TransientSubmissionTask(this.id, 
                                           this.target,
                                           this.method, 
                                           this.entity, 
                                           this.rejectStatusList,  
                                           this.processDelays, 
                                           numTrials, 
                                           dateLastTrial,
                                           this.submission);
    }

    public Submission getSubmission() {
        return submission;
    }

    protected void onTerminate() {  }
    
    protected void onReleased() {  }             

    @Override
    public String toString() {
        return id + " - " + method + " " + target;
    }
    
    public CompletableFuture<Optional<TransientSubmissionTask>> processAsync(final Client httpClient, final ScheduledThreadPoolExecutor executor) {
        final SubmitTask task = new SubmitTask(httpClient);
        executor.schedule(task, nextExecutionDelay.toMillis(), TimeUnit.MILLISECONDS);
        return task;
    }
    
    private class SubmitTask extends CompletableFuture<Optional<TransientSubmissionTask>> implements Runnable {
        private final Client httpClient;
        
        public SubmitTask(final Client httpClient) {
            this.httpClient = httpClient;
        }
        
        @Override
        public void run() {
            LOG.debug("performing submission " + id + " (" + (numTrials + 1) + " of " + processDelays.size() + ")");
            queryExecutor.performHttpQuery(httpClient, method, target, entity)
                         .whenComplete((content, error) -> { 
                                                             try {         
                                                                 // success
                                                                 if (error == null) {
                                                                     LOG.debug("submission " + id + " executed successfully");
                                                                     submission.update(State.COMPLETED);
                                                                     onTerminate();
                                                                     complete(Optional.empty());
                                                                                    
                                                                 // error
                                                                 } else {
                                                                     if (rejectStatusList.contains(toStatus(error))) {
                                                                         LOG.warn("submission " + id + " failed. Discarding it", error);
                                                                         submission.update(State.DISCARDED);
                                                                         onTerminate();
                                                                         throw Exceptions.propagate(error);
                                                                                        
                                                                     } else {
                                                                         LOG.debug("submission " + id + " failed with " + toStatus(error));
                                                                         Optional<TransientSubmissionTask> nextRetry = nextRetry();
                                                                         if (nextRetry.isPresent()) {
                                                                             complete(nextRetry);
                                                                         } else {
                                                                             LOG.warn("no retries (of " + processDelays.size() + ") left for submission " + id + " discarding it");
                                                                             onTerminate();
                                                                             throw Exceptions.propagate(error);
                                                                         }
                                                                     }
                                                                 }
                                                                                
                                                             } catch (RuntimeException rt) {
                                                                 completeExceptionally(rt);
                                                             }
                                                           });
        }
    }
    
    private int toStatus(final Throwable exception) {
        if (exception == null) {
            return 200;
        } else {
            Throwable rootError = Exceptions.unwrap(exception);
            LOG.warn("performing submission " + id + " failed", rootError.getMessage());
            return (rootError instanceof WebApplicationException) ? ((WebApplicationException) rootError).getResponse().getStatus() 
                                                                  : 500;
        }
    }
    
    private Optional<TransientSubmissionTask> nextRetry() {
        final int nextTrial = numTrials + 1;
        if (nextTrial < processDelays.size()) {
            return Optional.of(cloneSubmissionTask(numTrials + 1, 
                                                   Instant.now()));
        } else {
            return Optional.empty();
        }
    }
    

    
    protected static final class SubmissionImpl implements Submission {
        private final AtomicReference<State> stateRef = new AtomicReference<>(State.PENDING);
        
        @Override
        public State getState() {
            return stateRef.get();
        }
        
        void update(State newState) {
            stateRef.set(newState);
        }
    }  
    
  
    
    private static class QueryExecutor {
        
        public CompletableFuture<String> performHttpQuery(final Client httpClient,
                                                          final Method method, 
                                                          final URI target, 
                                                          final Entity<?> entity) {
            final InvocationPromise promise = new InvocationPromise();
            try {
                final Builder builder = httpClient.target(target).request();
                if (method == Method.POST) {
                    builder.async().post(entity, promise);
                } else {
                    builder.async().put(entity, promise);
                }
            } catch (RuntimeException rt) {
                promise.completeExceptionally(rt);
            }     
            
            return promise;
        }

        private static final class InvocationPromise extends CompletableFuture<String> implements InvocationCallback<String> {
            
            @Override
            public void failed(final Throwable ex) {
                completeExceptionally(Exceptions.propagate(ex));
            }

            @Override
            public void completed(String content) {
                complete(content);
            }
        }
    }
}