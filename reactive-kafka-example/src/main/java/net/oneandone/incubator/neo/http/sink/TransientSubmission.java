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
import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.ResponseProcessingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import net.oneandone.incubator.neo.exception.Exceptions;
import net.oneandone.incubator.neo.http.sink.HttpSink.Method;
import net.oneandone.incubator.neo.http.sink.HttpSink.Submission;


/**
 * Transient submission task which lives in main memory only
 *
 */
class TransientSubmission implements Submission {
    private static final Logger LOG = LoggerFactory.getLogger(TransientSubmission.class);

    private final String id;
    private final URI target;
    private final Entity<?> entity;
    private final ImmutableSet<Integer> rejectStatusList;
    private final ImmutableList<Duration> processDelays;
    private final Method method;

    private final AtomicReference<State> stateRef = new AtomicReference<>(State.PENDING);

    
    /**
     * @param id                 the id
     * @param target             the target uri
     * @param method             the method
     * @param entity             the entity 
     * @param rejectStatusList   the reject status list
     * @param processDelays      the process delays
     */
    public TransientSubmission(final String id,
    						   final URI target,
    						   final Method method,
    						   final Entity<?> entity, 
    						   final ImmutableSet<Integer> rejectStatusList,
    						   final ImmutableList<Duration> processDelays) {
    	this.id = id;
    	this.target = target;
    	this.entity = entity;
    	this.rejectStatusList = rejectStatusList;
    	this.processDelays = processDelays;
    	this.method = method;
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
    
    void update(State newState) {
    	stateRef.set(newState);
    }
    
    ImmutableList<Duration> getProcessDelays() {
    	return processDelays;
    }
    
	@Override
	public String toString() {
		return id + " - " + method + " " + target;
	}
	
	/**
	 * opens the submission  
	 * @return the submission task to execute
	 */
	CompletableFuture<SubmissionTask> openAsync() {
		return CompletableFuture.completedFuture(new TransientSubmissionTask());    	
    }    
	 

	/**
	 * Transient submission task
	 */
	protected class TransientSubmissionTask implements SubmissionTask {
	    private final int numTrials;
	    private final Duration nextExecutionDelay;
	    
	    /**
	     * constructor
	     */
	    TransientSubmissionTask() {
	    	this(0,                   // no trials performed yet													
			     Instant.now());      // last trial time is now (time starting point is now)))
	    }
	    
	    /**
	     * constructor
	     * @param numTrials      the current trial nunmber
	     * @param dateLastTrial  the data last trial
	     */
	    TransientSubmissionTask(final int numTrials, final Instant dateLastTrial) {
	        this.numTrials = numTrials;

	        final Duration nextDelay = getProcessDelays().get(numTrials); 
	        final Duration elapsedSinceLastRetry = Duration.between(dateLastTrial, Instant.now());
	        final Duration correctedDelay = nextDelay.minus(elapsedSinceLastRetry);
	        this.nextExecutionDelay = correctedDelay.isNegative() ? Duration.ZERO : correctedDelay;
	    }

	    @Override
	    public Submission getSubmission() {
	    	return TransientSubmission.this;
	    }
	    
	    @Override
	    public int getNumTrials() {
	    	return numTrials;
	    }
		
	    @Override
	    public CompletableFuture<Optional<SubmissionTask>> processAsync(final QueryExecutor executor) {
	    	LOG.debug(subInfo() + " will be executed in " + nextExecutionDelay);
	        
	        return executor.performHttpQueryAsync(subInfo(), getMethod(), getTarget(), getEntity(), nextExecutionDelay)
	        			   .thenApply(httpBody -> { 	// success
	        				   							LOG.debug(subInfo() + " executed successfully");
	        				   							complete();
	        				   							return Optional.<SubmissionTask>empty();
	        			   						  })
	        			   .exceptionally(error -> {	// error
	        				   							error = unwrap(error);
	        				   							if (getRejectStatusList().contains(toStatus(error))) {
	        				   								LOG.warn(subInfo() + " failed. Discarding it", error);
	        				   								discard();
	        				   								throw Exceptions.propagate(error);
	        				   							} else {
	        				   								LOG.debug(subInfo() + " failed with " + toStatus(error));
	        				   								Optional<SubmissionTask> nextRetry = nextRetry();
	        				   								if (nextRetry.isPresent()) {
	        				   									return nextRetry;
	        				   								} else {
	        				   									LOG.warn("no retries left for " + subInfo() + " discarding it");
	        				   									discard();
	        				   									throw Exceptions.propagate(error);
	        				   								}
	        				   							}
	        			   						  });
	    }

	    private void complete() {
	    	update(State.COMPLETED);
	    	onTerminated();
	    }
	    
	    private void discard() {
	    	update(State.DISCARDED);
	    	onTerminated();
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
	    
	    private Optional<SubmissionTask> nextRetry() {
	        final int nextTrial = numTrials + 1;
	        if (nextTrial < getProcessDelays().size()) {
	            return Optional.of(copySubmissionTask(numTrials + 1, Instant.now()));
	        } else {
	            return Optional.empty();
	        }
	    }
	    
	    /**
	     * copies the submission
	     * 
	     * @param numTrials       the current number of trial to use
	     * @param dateLastTrial   the last trial date to use
	     * @return the copied submission task
	     */
	    protected SubmissionTask copySubmissionTask(final int numTrials, final Instant dateLastTrial) {
	    	return new TransientSubmissionTask(numTrials, dateLastTrial);
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