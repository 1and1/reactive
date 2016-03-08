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

import java.util.Optional;

import java.util.concurrent.CompletableFuture;

import net.oneandone.incubator.neo.http.sink.HttpSink.Submission;


/**
 * The submission task. A submission task represents a (re)try of the submission execution
 *
 */
interface SubmissionTask {
	
	/**
	 * @return the associated submission
	 */
    Submission getSubmission();
	
    /**
     * releases the task (e.g. releases the underlying file lock) for later processing
     */
	default void onReleased() { }     
	
	/**
	 * terminates the task by destroying it
	 */
    default void onTerminated() {  }
	
	/**
	 * processes the task asynchronously 
	 * @param queryExecutor  the query executor
	 * @return the succeeding retry task or empty
	 */
    CompletableFuture<Optional<SubmissionTask>> processAsync(QueryExecutor executor);
  
    /**
     * @return the number of trials
     */
    int getNumTrials();
}