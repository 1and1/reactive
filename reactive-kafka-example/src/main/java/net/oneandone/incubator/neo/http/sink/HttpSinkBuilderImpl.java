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

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import net.oneandone.incubator.neo.collect.Immutables;
import net.oneandone.incubator.neo.http.sink.HttpSink.Method;
import net.oneandone.incubator.neo.http.sink.PersistentSubmission.SubmissionDir;


/**
 * HttpSinklBuilder implementation
 */
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

 
    /**
     * @param userClient         the user client or null
     * @param target             the target uri
     * @param method             the method to use
     * @param bufferSize         the retry buffer size
     * @param dir                the persistency dir  
     * @param rejectStatusList   the reject status list
     * @param retryDelays        the retry delays
     * @param numParallelWorkers the num parallel workers
     */
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
    
    @Override
    public HttpSinkBuilder withRejectOnStatus(Integer... rejectStatusList) {
        Preconditions.checkNotNull(rejectStatusList);
        return withRejectOnStatus(ImmutableSet.copyOf(rejectStatusList));
    }
    
    @Override
    public HttpSink open() {
        // if dir is set, sink will run in persistent mode  
        return (dir == null) ? new TransientHttpSink() : new PersistentHttpSink();
    }   
    
    
    private class TransientHttpSink implements HttpSink {
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        private final Optional<Client> clientToClose;
        protected final QueryExecutor queryExecutor;
        protected final SubmissionMonitor submissionMonitor = new SubmissionMonitor();
        
        
        public TransientHttpSink() {
             // using default client?
             if (userClient == null) {
                 final Client defaultClient = ClientBuilder.newClient();
             	this.queryExecutor = new QueryExecutor(defaultClient, numParallelWorkers);
                 this.clientToClose = Optional.of(defaultClient);
                 
             // .. no client is given by user 
             } else {
             	this.queryExecutor = new QueryExecutor(userClient, numParallelWorkers);
                 this.clientToClose = Optional.empty();
             }
		}

        @Override
        public Metrics getMetrics() {
        	return submissionMonitor;
        }
        
        @Override
        public boolean isOpen() {
            return isOpen.get();
        }
        
        @Override
        public void close() {
        	submissionMonitor.getPendingSubmissions().forEach(submission -> ((TransientSubmission) submission).release()); // release pending submissions
            clientToClose.ifPresent(client -> client.close());                                                             // close http client if default client 
            queryExecutor.close();													
        } 
        
        @Override
        public ImmutableSet<Submission> getPendingSubmissions() {
        	return submissionMonitor.getPendingSubmissions();
        }
        
        @Override
        public String toString() {
            return new StringBuilder().append(method + " " + target + "\r\n")
            						  .append("success=" + getMetrics().getNumSuccess().getCount())
                                      .append("retries=" + getMetrics().getNumRetries().getCount())
                                      .append("discarded=" + getMetrics().getNumDiscarded().getCount())
                                      .toString();
        }
        
        @Override
        public CompletableFuture<Submission> submitAsync(final Object entity, final MediaType mediaType) {
        	return submitAsync(newSubmission(UUID.randomUUID().toString(),
        									 target, 
        									 method,
        									 Entity.entity(entity, mediaType), 
        									 rejectStatusList,
        									 Immutables.join(Duration.ofMillis(0), retryDelays)));  // add first trial (which is not a retry)
        }
        
        /**
         * submits the submission
         * @param submission the submission to submit
         * @return the submission future
         */
        protected CompletableFuture<Submission> submitAsync(final TransientSubmission submission) {
        	if (submissionMonitor.getNumPendingSubmissions() > bufferSize) {
        		throw new IllegalStateException("max buffer size " + bufferSize + " execeeded");
        	}
        	return submission.processAsync(queryExecutor);
        }
       
        protected TransientSubmission newSubmission(final String id,
				   					         	    final URI target,
				   					         	    final Method method,
				   					         	    final Entity<?> entity, 
				   					         	    final ImmutableSet<Integer> rejectStatusList,
				   					         	    final ImmutableList<Duration> processDelays) {
        	return new TransientSubmission(submissionMonitor, 
        								   id,
        								   target, 
        								   method,
        								   entity, 
        								   rejectStatusList,
        								   processDelays);    	
        }
    }      
     
    
    final class PersistentHttpSink extends TransientHttpSink {
    	private final PersistentSubmission.SubmissionsStore submissionsStore;
    	
        public PersistentHttpSink() {
            super();
            this.submissionsStore = new PersistentSubmission.SubmissionsStore(dir, target, method);

            // process old submissions (if exists)
            submissionsStore.scanUnprocessedSubmissionDirs()
            			    .forEach(submissionDir -> submissionDir.getNewestSubmissionFile()
            				  	  							       .ifPresent(file -> submitRetry(submissionDir, file)));
        }
        
        private void submitRetry(final SubmissionDir submissionDir, final File submissionFile) {
        	final PersistentSubmission submission = PersistentSubmission.load(submissionMonitor, submissionDir, submissionFile);
        	LOG.warn("pending persistent submission file " + submissionFile + " found. rescheduling it");
        	submissionMonitor.onRetry(submission);
        	submitAsync(submission);
        }
        
        public File getSubmissionStoreDir() {
        	return submissionsStore.asFile();
        }

        @Override
        protected TransientSubmission newSubmission(final String id,
	         	    								final URI target,
	         	    								final Method method,
	         	    								final Entity<?> entity, 
	         	    								final ImmutableSet<Integer> rejectStatusList,
	         	    								final ImmutableList<Duration> processDelays) {
        	return new PersistentSubmission(submissionMonitor, 
        									id, 
        								    target,
        								    method, 
        								    entity, 
        								    rejectStatusList, 
        								    processDelays,
        								    submissionsStore);    	
        }
    } 
}