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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.ResponseProcessingException;

import org.glassfish.hk2.runlevel.RunLevelException;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import net.oneandone.incubator.neo.exception.Exceptions;
import net.oneandone.incubator.neo.http.sink.HttpSink.Method;
import net.oneandone.incubator.neo.http.sink.HttpSink.Submission;


/**
 * Transient submission task which lives in main memory only
 *
 */
class TransientSubmission implements Submission {
    private static final Logger LOG = LoggerFactory.getLogger(TransientSubmission.class);

    private final SubmissionMonitor submissionMonitor;
    private final String id;
    private final URI target;
    private final Entity<?> entity;
    private final ImmutableSet<Integer> rejectStatusList;
    private final ImmutableList<Duration> processDelays;
    private final Method method;

    // state
    private final AtomicReference<State> stateRef = new AtomicReference<>(State.PENDING);
    private final AtomicBoolean isReleased = new AtomicBoolean(false);
    private final CopyOnWriteArrayList<Instant> lastTrials;
    private final CopyOnWriteArrayList<String> actionLog;

    
    /**
     * @param submissionMonitor  the submission monitor
     * @param id                 the id
     * @param target             the target uri
     * @param method             the method
     * @param entity             the entity 
     * @param rejectStatusList   the reject status list
     * @param processDelays      the process delays
     */
    public TransientSubmission(final SubmissionMonitor submissionMonitor,
		    				   final String id,
    						   final URI target,
    						   final Method method,
    						   final Entity<?> entity, 
    						   final ImmutableSet<Integer> rejectStatusList,
    						   final ImmutableList<Duration> processDelays) {
    	this(submissionMonitor,
    		 id,
    		 target, 
    		 method, 
    		 entity, 
    		 rejectStatusList, 
    		 processDelays, 
    		 ImmutableList.of(),
    		 ImmutableList.of());
    }
    
    /**
     * @param submissionMonitor  the submission monitor
     * @param id                 the id
     * @param target             the target uri
     * @param method             the method
     * @param entity             the entity 
     * @param rejectStatusList   the reject status list
     * @param processDelays      the process delays
     * @param lastTrials         the date of the last trials
     * @param actionLog          the action log
     */
    protected TransientSubmission(final SubmissionMonitor submissionMonitor,
		      				      final String id,
		      				      final URI target,
		      				      final Method method,
		      				      final Entity<?> entity, 
		      				      final ImmutableSet<Integer> rejectStatusList,
		      				      final ImmutableList<Duration> processDelays,
		      				      final ImmutableList<Instant> lastTrials,
		      				      final ImmutableList<String> actionLog) {
    	this.submissionMonitor = submissionMonitor;
    	this.id = id;
    	this.target = target;
    	this.entity = entity;
    	this.rejectStatusList = rejectStatusList;
    	this.processDelays = processDelays;
    	this.method = method;
    	this.lastTrials = Lists.newCopyOnWriteArrayList(lastTrials);
    	this.actionLog = Lists.newCopyOnWriteArrayList(actionLog);
    	submissionMonitor.register(this);
    }
        
    @Override
    public String getId() {
    	return id;
    }

    @Override
    public URI getTarget() {
    	return target;
    }

    @Override
    public Method getMethod() {
    	return method;
    }
		
    @Override
    public Entity<?> getEntity() {
    	return entity;
    }
        
    @Override
    public ImmutableSet<Integer> getRejectStatusList() {
    	return rejectStatusList;
    }

    @Override
    public State getState() {
    	return stateRef.get();
    }
    
    ImmutableList<Instant> getLastTrials() {
    	return ImmutableList.copyOf(lastTrials);
    }
    
    ImmutableList<Duration> getProcessDelays() {
    	return processDelays;
    }
    
    ImmutableList<String> getActionLog() {
    	return ImmutableList.copyOf(actionLog);
    }
    
	@Override
	public String toString() {
		return id + " - " + method + " " + target + " (" + stateRef.get() + ")\r\nlog:\r\n" + Joiner.on("\r\n").join(actionLog);
	}

	void release() {
		if (!isReleased.getAndSet(true)) {
			submissionMonitor.deregister(this);
			onReleased();
		}
	}
	
	void onReleased() { }
	
	/**
	 * processes the submission
	 * @param executor   the query executor
	 * @return the submission future
	 */
	public CompletableFuture<Submission> processAsync(final QueryExecutor executor) {
		return newTaskAsync(lastTrials.size()).thenCompose(submissionTask -> processTaskAsync(executor, submissionTask))    
				 	         				  .thenApply(optionalRetryTask -> TransientSubmission.this);	
    }    
	
    /**
     * @param executor       the query executor
     * @param submissionTask the submission task to process
     * @return the submission future
     */
    public CompletableFuture<TransientSubmissionTask> processTaskAsync(final  QueryExecutor executor,
    														           final TransientSubmissionTask submissionTask) {
        LOG.debug("submitting " + submissionTask); 
        return processSubmissionTaskAsync(executor, submissionTask);
    }

    /**
     * @param executor       the query executor
     * @param submissionTask the submission retry task
     */
    void processRetryAsync(final QueryExecutor executor, final TransientSubmissionTask submissionTask) {
    	processSubmissionTaskAsync(executor, submissionTask)
    			.whenComplete((sub, error) -> submissionMonitor.onRetry(TransientSubmission.this));
    }

    private CompletableFuture<TransientSubmissionTask> processSubmissionTaskAsync(final QueryExecutor executor, 
    																			  final TransientSubmissionTask submissionTask) {
        return submissionTask.processAsync(executor)
                             .exceptionally(error -> { throw Exceptions.propagate(error); })
                             .thenApply(optionalNextRetry ->  { 
                                                                 if (optionalNextRetry.isPresent()) {   
                                                                     processRetryAsync(executor, optionalNextRetry.get()); 
                                                                 } 
                                                                 return submissionTask;
                                                              }); 
    }

	protected CompletableFuture<TransientSubmissionTask> newTaskAsync(final int numTrials) {
		return CompletableFuture.completedFuture(new TransientSubmissionTask(numTrials));
	}
	
	
	/**
	 * Transient submission task
	 */
	protected class TransientSubmissionTask {
	    private final int numTrials;
	    private final Duration nextExecutionDelay;
	    
	    /**
	     * constructor
	     * @param numTrials      the current trial number
	     */
	    TransientSubmissionTask(int numTrials) {
	        this.numTrials = numTrials;

	        final Duration nextDelay = getProcessDelays().get(numTrials);
	        final Duration elapsedSinceLastRetry = (lastTrials.isEmpty()) ? Duration.ofMillis(0)
	        		                                                      : Duration.between(lastTrials.get(lastTrials.size() -1), Instant.now());
	        final Duration correctedDelay = nextDelay.minus(elapsedSinceLastRetry);
	        this.nextExecutionDelay = correctedDelay.isNegative() ? Duration.ZERO : correctedDelay;
	    }
	    
		/**
		 * termination call back method to perform cleanup tasks
		 * @param isSuccess  true, is processing was successfully
		 */
	    protected void onTerminated(final boolean isSuccess) { 
	    	if (isSuccess) {
	    		stateRef.set(State.COMPLETED);
		    	submissionMonitor.onSuccess(TransientSubmission.this);
	    	} else {
	    		stateRef.set(State.DISCARDED);
		    	submissionMonitor.onDiscarded(TransientSubmission.this);
	    	}
	    }

	    /**
		 * processes the task asynchronously 
		 * @param queryExecutor  the query executor
		 * @return the succeeding retry task or empty
		 */
	    public CompletableFuture<Optional<TransientSubmissionTask>> processAsync(final QueryExecutor executor) {
	    	if (isReleased.get()) {
	    		throw new RunLevelException(subInfo() + " already released. Task will not been processed");
	    	}
	    	LOG.debug(subInfo() + " will be executed in " + nextExecutionDelay);
	        
	        return executor.performHttpQueryAsync(subInfo(), getMethod(), getTarget(), getEntity(), nextExecutionDelay)
	        			   .thenApply(httpBody -> onSuccess())  
	        			   .exceptionally(error -> onError(unwrap(error)));
	    }
	    
	    private Optional<TransientSubmissionTask> onSuccess() {
	    	lastTrials.add(Instant.now());
	    	logSuccess("executed successfully");
	    	onTerminated(true);
			return Optional.<TransientSubmissionTask>empty();
	    }
	    
	    void logSuccess(String msg) {
	    	actionLog.add("[" + Instant.now() + "] " + subInfo() + " " + msg);
	    	Log.debug(subInfo() + " " + msg);
	    }
	    
	    void logError(String msg) {
			actionLog.add("[" + Instant.now() + "] " + subInfo() + " " + msg);
	    	Log.warn(subInfo() + " " + msg);
	    }
	    

	    
	    private Optional<TransientSubmissionTask> onError(final Throwable error) {
	    	lastTrials.add(Instant.now());
	    	
			if (getRejectStatusList().contains(toStatus(error))) {
				logError("failed with " + error.getMessage() + ". Non retryable status code. Discarding it");
		    	onTerminated(false);
				throw Exceptions.propagate(error);
				
			} else {
				Optional<TransientSubmissionTask> nextRetry = nextRetry();
				if (nextRetry.isPresent()) {
					logError("failed with " + error.getMessage() + ". Retries left");
			    	return nextRetry;
				} else {
					logError("failed with " + error.getMessage() + ". No retries left. Discarding it");
			    	onTerminated(false);
					throw Exceptions.propagate(error);
				}
			}
	    }
	    
	    private Throwable unwrap(Throwable error) {
	    	Throwable rootError = Exceptions.unwrap(error);
			if (rootError instanceof ResponseProcessingException) {
				rootError = ((ResponseProcessingException) rootError).getCause();
			}
			return rootError;
	    }
    
	    private int toStatus(final Throwable error) {
	        if (error == null) {
	            return 200;
	        } else {
	            return (error instanceof WebApplicationException) ? ((WebApplicationException) error).getResponse().getStatus() 
	                                                              : 500;
	        }
	    }
	    
	    private Optional<TransientSubmissionTask> nextRetry() {
	        final int nextTrial = numTrials + 1;
	        if (nextTrial < getProcessDelays().size()) {
	            return Optional.of(newTask(numTrials + 1));
	        } else {
	            return Optional.empty();
	        }
	    }
	    
	    protected TransientSubmissionTask newTask(final int numTrials) {
	    	return new TransientSubmissionTask(numTrials);
	    }
	    
	    private String subInfo() {
	    	return "submission " + getId() + " (" + (numTrials + 1) + " of " + getProcessDelays().size() + ")";
	    }
	    
	    @Override
	    public String toString() {
	    	return TransientSubmission.this.toString();
	    }
	}
}  