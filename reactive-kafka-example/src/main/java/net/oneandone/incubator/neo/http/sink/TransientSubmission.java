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

import javax.ws.rs.client.Entity;

import org.glassfish.hk2.runlevel.RunLevelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import net.oneandone.incubator.neo.http.sink.HttpSink.Method;
import net.oneandone.incubator.neo.http.sink.HttpSink.Submission;
import net.oneandone.incubator.neo.http.sink.QueryExecutor.QueryResponse;


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
		return newTaskAsync(lastTrials.size()).thenCompose(submissionTask -> submissionTask.processAsync(executor));	
    }    

    /**
     * @param numTrials  the num trials
     * @return the new task
     */
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
		 * processes the task asynchronously 
		 * @param queryExecutor  the query executor
		 * @return the submission future
		 */
	    public CompletableFuture<Submission> processAsync(final QueryExecutor executor) {
	    	if (isReleased.get()) {
	    		throw new RunLevelException(subInfo() + " is already released. Task will not been processed");
	    	}
	    	
	    	LOG.debug(subInfo() + " will be executed in " + nextExecutionDelay);
	        return executor.performHttpQueryAsync(subInfo(), getMethod(), getTarget(), getEntity(), nextExecutionDelay)
	        			   .thenApply(response -> {
	        				   						lastTrials.add(Instant.now());
	        				   						if (response.isSuccess()) {                                         // success
	        				   							onSuccess("executed with " + response);
	        				   						} else if (getRejectStatusList().contains(response.getStatus())) {  // no retryable error
	        				   							throw onDiscard("Non retryable status code", response);    
	        				   						} else if (!onRetry(response, executor)) {                          // try retry
	        				   							throw onDiscard("No retries left", response);                   // no retry left
	        				   						}
	        				   						return TransientSubmission.this;
	        			   						  });
	    }
	    
	    protected void onSuccess(final String msg) { 
	    	stateRef.set(State.COMPLETED);
	    	submissionMonitor.onSuccess(TransientSubmission.this);
	    	
	    	actionLog.add("[" + Instant.now() + "] " + subInfo() + " " + msg);
	    	LOG.debug(subInfo() + " " + msg);
	    }

	    protected boolean onRetry(final QueryResponse response, final QueryExecutor executor) {
	    	Optional<TransientSubmissionTask> nextRetry = nextRetry();
	    	// retries left?
	    	if (nextRetry.isPresent()) {
		    	actionLog.add("[" + Instant.now() + "] " + subInfo() + " failed with " + response + ". Retries left");
		    	LOG.warn(subInfo() + " failed with " + response + ". Retries left");
		    	nextRetry.get()
		    			 .processAsync(executor)
		    			 .whenComplete((sub, ex) -> submissionMonitor.onRetry(TransientSubmission.this));
		    	return true;
	    	} else {
	    		return false;
	    	}
	    }

	    protected RuntimeException onDiscard(final String msg, final QueryResponse response) { 
	    	stateRef.set(State.DISCARDED);
	    	submissionMonitor.onDiscarded(TransientSubmission.this);
	    	final String fullMsg = "failed with " + response + ". " + msg + ". Discarding it";
	    	actionLog.add("[" + Instant.now() + "] " + subInfo() + " " + fullMsg);
	    	LOG.warn(subInfo() + " " + fullMsg);
	    	return response.getError();
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