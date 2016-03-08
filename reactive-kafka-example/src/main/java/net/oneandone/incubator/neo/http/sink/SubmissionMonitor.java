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

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import net.oneandone.incubator.neo.http.sink.HttpSink.Submission;


/**
 * HttpSinklBuilder implementation
 */
final class SubmissionMonitor implements HttpSink.Metrics {
    private final MetricRegistry metrics = new MetricRegistry();
    private final Counter success = metrics.counter("success");
    private final Counter retries = metrics.counter("retries");
    private final Counter discarded = metrics.counter("discarded");

	private final Set<Submission> runningSubmissions = Collections.newSetFromMap(new WeakHashMap<Submission, Boolean>());

	public void register(final Submission submission) {
		synchronized (this) {
			runningSubmissions.add(submission);
		}
	}
	
	public void deregister(final Submission submission) {
		synchronized (this) {
			runningSubmissions.remove(submission);
		}
	}
	
	/**
	 * @return the pending submissions
	 */
	public ImmutableSet<Submission> getPendingSubmissions() {
		synchronized (this) {
			return ImmutableSet.copyOf(runningSubmissions);
		}
	}
	
	/**
	 * @return num pending submissions
	 */
	public int getNumPendingSubmissions() {
		synchronized (this) {
			return runningSubmissions.size();
		}
	}
	
	void onDiscarded(final Submission submission) {
		discarded.inc();
		deregister(submission);
	}
	
	@Override
	public Counter getNumDiscarded() {
		return discarded;
	}

	void onRetry(final Submission submission) {
		retries.inc();
	}

	@Override
	public Counter getNumRetries() {
		return retries;
	}

	void onSuccess(final Submission submission) {
		success.inc();
		deregister(submission);
	}
	
	@Override
	public Counter getNumSuccess() {
		return success;
	}
}