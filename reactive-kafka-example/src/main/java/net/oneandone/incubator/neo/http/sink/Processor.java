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


/**
 * Submission task processor
 */
final class Processor implements Closeable, HttpSink.Metrics {
    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);
    
    private final SubmissionTaskMonitor taskMonitor = new SubmissionTaskMonitor();
    private final MetricRegistry metrics = new MetricRegistry();
    private final Counter success = metrics.counter("success");
    private final Counter retries = metrics.counter("retries");
    private final Counter discarded = metrics.counter("discarded");
    
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final ScheduledThreadPoolExecutor executor;    
    private final int maxQueueSize;
    private final QueryExecutor queryExecutor;
    private final Optional<Client> clientToClose;
    
    /**
     * @param userClient          the user client or null 
     * @param numParallelWorkers  the num parallel workers
     * @param maxQueueSize        the max queue size
     */
    Processor(final Client userClient, final int numParallelWorkers, final int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
        this.executor = new ScheduledThreadPoolExecutor(numParallelWorkers);
        
        // using default client?
        if (userClient == null) {
            final Client defaultClient = ClientBuilder.newClient();
        	this.queryExecutor = new QueryExecutor(defaultClient, executor);
            this.clientToClose = Optional.of(defaultClient);
            
        // .. no client is given by user 
        } else {
        	this.queryExecutor = new QueryExecutor(userClient, executor);
            this.clientToClose = Optional.empty();
        }
    }

    /**
     * @return true, if is open
     */
    public boolean isOpen() {
        return isOpen.get();
    }
    
    @Override
    public void close() {
    	taskMonitor.getPendingSubmissions().forEach(task -> task.onReleased()); // release pending submission tasks
        clientToClose.ifPresent(client -> client.close());                   // close http client if default client 
        executor.shutdown();													
    }

    /**
     * @return the pending submissions
     */
    public ImmutableSet<Submission> getPendingSubmissions() {
    	return taskMonitor.getPendingSubmissions().stream()
                								  .map(submission -> submission.getSubmission())
                								  .collect(Immutables.toSet());
    }

    /**
     * @param submissionTask the submission task to process
     * @return the submission future
     */
    public CompletableFuture<SubmissionTask> processTaskAsync(final SubmissionTask submissionTask) {
        if (taskMonitor.getNumPendingSubmissions() >= maxQueueSize) {
            throw new IllegalStateException("max queue size " + maxQueueSize + " exceeded");
        }
        
        LOG.debug("submitting " + submissionTask); 
        return processSubmissionTaskAsync(submissionTask);
    }

    /**
     * @param submissionTask the submission retry task
     */
    void processRetryAsync(final SubmissionTask submissionTask) {
    	processSubmissionTaskAsync(submissionTask).whenComplete((sub, error) -> retries.inc());
    }

    private CompletableFuture<SubmissionTask> processSubmissionTaskAsync(final SubmissionTask submissionTask) {
        if (!isOpen()) {
            throw new IllegalStateException("processor is already closed");
        }
        
        return processTaskMonitoredAsync(submissionTask)
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
    
    private CompletableFuture<Optional<SubmissionTask>> processTaskMonitoredAsync(final SubmissionTask submissionTask) {
        taskMonitor.register(submissionTask);
        try {
	        return submissionTask.processAsync(queryExecutor)
	                             .handle((optionalNextRetry, error) ->  {
	                            	 										 taskMonitor.deregister(submissionTask);
	                                                                         if (error == null) {
	                                                                             return optionalNextRetry;
	                                                                         } else {
	                                                                             throw Exceptions.propagate(error);
	                                                                         }
	                                                                    });
        } catch (RuntimeException rt) {
        	taskMonitor.deregister(submissionTask);
        	throw rt;
        }
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
    public String toString() {
        return new StringBuilder().append("success=" + getNumSuccess().getCount())
                                  .append("retries=" + getNumRetries().getCount())
                                  .append("discarded=" + getNumDiscarded().getCount())
                                  .toString();
    }
    
    
    private final class SubmissionTaskMonitor {
    	private final Set<SubmissionTask> runningSubmissions = Collections.newSetFromMap(new WeakHashMap<SubmissionTask, Boolean>());
        
        private synchronized void register(final SubmissionTask submission) {
        	runningSubmissions.add(submission);
        }
        
        private synchronized void deregister(final SubmissionTask submission) {
        	runningSubmissions.remove(submission);
        }
        
        /**
         * @return the pending submissions
         */
        public synchronized ImmutableSet<SubmissionTask> getPendingSubmissions() {
        	return ImmutableSet.copyOf(runningSubmissions);
        }
        
        private synchronized int getNumPendingSubmissions() {
        	return runningSubmissions.size();
        }
    }   
}