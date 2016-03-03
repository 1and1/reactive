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

import java.io.Closeable;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;

import net.oneandone.incubator.neo.collect.Immutables;
import net.oneandone.incubator.neo.exception.Exceptions;
import net.oneandone.incubator.neo.http.sink.HttpSink.Submission;


final class Processor implements Closeable, HttpSink.Metrics {
    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);
    
    private final Set<SubmissionTask> runningSubmissions = Collections.newSetFromMap(new WeakHashMap<SubmissionTask, Boolean>());
    private final MetricRegistry metrics = new MetricRegistry();
    private final Counter success = metrics.counter("success");
    private final Counter retries = metrics.counter("retries");
    private final Counter discarded = metrics.counter("discarded");
    
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final ScheduledThreadPoolExecutor executor;    
    private final int maxQueueSize;
    private final QueryExecutor queryExecutor;
    private final Optional<Client> clientToClose;
    
    
    Processor(final Client userClient, final int numParallelWorkers, final int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
        this.executor = new ScheduledThreadPoolExecutor(numParallelWorkers);
        
        // using default client?
        if (userClient == null) {
            final Client defaultClient = ClientBuilder.newClient();
        	this.queryExecutor = new QueryExecutor(defaultClient);
            this.clientToClose = Optional.of(defaultClient);
            
        // .. no client is given by user 
        } else {
        	this.queryExecutor = new QueryExecutor(userClient);
            this.clientToClose = Optional.empty();
        }
    }
    
    @Override
    public void close() {
    	runningSubmissions.forEach(submissionTask -> submissionTask.release()); // release pending submissions
        clientToClose.ifPresent(client -> client.close());                      // close http client if default client 
        executor.shutdown();													
    }

    /**
     * @return the pending submissions
     */
    public ImmutableSet<Submission> getPendingSubmissions() {
    	return runningSubmissions.stream()
                                 .map(submission -> submission.getSubmission())
                                 .collect(Immutables.toSet());
    }
    
    /**
     * @return true, if is open
     */
    public boolean isOpen() {
        return isOpen.get();
    }
    
    public CompletableFuture<SubmissionTask> processTaskAsync(final SubmissionTask submissionTask) {
        if (getNumPending() >= maxQueueSize) {
            throw new IllegalStateException("max queue size " + maxQueueSize + " exceeded");
        }
        
        LOG.debug("submitting " + submissionTask); 
        return processSubmissionTaskAsync(submissionTask);
    }

    public void processRetryAsync(final SubmissionTask submissionTask) {
    	processSubmissionTaskAsync(submissionTask).whenComplete((sub, error) -> retries.inc());
    }

    private CompletableFuture<SubmissionTask> processSubmissionTaskAsync(final SubmissionTask submissionTask) {
        return processSubmissionMonitoredAsync(submissionTask)
                             .exceptionally(error -> { 
                                                         discarded.inc();
                                                         throw Exceptions.propagate(error);
                                                     })
                             .thenApply(optionalNextRetry ->  { 
                                                                 if (optionalNextRetry.isPresent()) {   
                                                                     processRetryAsync(optionalNextRetry.get()); 
                                                                 } else {
                                                                     success.inc();                                                             
                                                                 }
                                                                 return submissionTask;
                                                              }); 
    }
    
    private CompletableFuture<Optional<SubmissionTask>> processSubmissionMonitoredAsync(final SubmissionTask submissionTask) {
        if (!isOpen()) {
            throw new IllegalStateException("processor is already closed");
        }
        
        register(submissionTask);
        return submissionTask.processAsync(queryExecutor, executor)
                             .handle((optionalNextRetry, error) ->  {
                                                                         deregister(submissionTask, optionalNextRetry);
                                                                         if (error == null) {
                                                                             return optionalNextRetry;
                                                                         } else {
                                                                             throw Exceptions.propagate(error);
                                                                         }
                                                                    }); 
    }
    
    private void register(final SubmissionTask submission) {
        synchronized (this) {
            runningSubmissions.add(submission);
        }
    }
    
    private <T> T deregister(final SubmissionTask submission, final T t) {
        synchronized (this) {
            runningSubmissions.remove(submission);
        }
        return t;
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
    public Counter getNumDiscarded() {
        return discarded;
    }
    
    @Override
    public int getNumPending() {
        synchronized (this) {
            return runningSubmissions.size();
        }
    }
    
    
    @Override
    public String toString() {
        return new StringBuilder().append("pending=" + getNumPending())
                                  .append("success=" + getNumSuccess().getCount())
                                  .append("retries=" + getNumRetries().getCount())
                                  .append("discarded=" + getNumDiscarded().getCount())
                                  .toString();
    }
}