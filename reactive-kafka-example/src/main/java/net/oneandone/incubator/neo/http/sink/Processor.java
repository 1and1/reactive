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

import net.oneandone.incubator.neo.exception.Exceptions;
import net.oneandone.incubator.neo.http.sink.HttpSink.Submission;



final class Processor implements Closeable, HttpSink.Metrics {
    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);
    
    private final Set<TransientSubmission> runningSubmissions = Collections.newSetFromMap(new WeakHashMap<TransientSubmission, Boolean>());
    private final MetricRegistry metrics = new MetricRegistry();
    private final Counter success = metrics.counter("success");
    private final Counter retries = metrics.counter("retries");
    private final Counter discarded = metrics.counter("discarded");
    
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final ScheduledThreadPoolExecutor executor;    
    private final Client httpClient;
    private final Optional<Client> clientToClose;
    
    
    Processor(Client userClient, int numParallelWorkers) {
        this.executor = new ScheduledThreadPoolExecutor(numParallelWorkers);
        
        // using default client?
        if (userClient == null) {
            final Client defaultClient = ClientBuilder.newClient();
            this.httpClient = defaultClient;
            this.clientToClose = Optional.of(defaultClient);
            
        // .. no client is given by user 
        } else {
            this.httpClient = userClient;
            this.clientToClose = Optional.empty();
        }
    }
    
    @Override
    public void close() {
        for (TransientSubmission query : runningSubmissions) {
            query.release();
        }

        clientToClose.ifPresent(c -> c.close());
        executor.shutdown();
    }

    public boolean isOpen() {
        return isOpen.get();
    }
    
    public CompletableFuture<Submission> process(final TransientSubmission submission) {
        if (!isOpen()) {
            throw new IllegalStateException("processor is already closed");
        }
        
        register(submission);
        
        return submission.process(httpClient, executor)
                         .thenApply(completed -> completed ? onSubmissionCompleted(submission) : onSubmissionFailed(submission))
                         .exceptionally(error -> onSubmissionDiscarded(submission, error));
    }

    public void processRetry(final TransientSubmission submission) {
        process(submission).whenComplete((sub, error) -> { retries.inc(); });
    }

    private Submission onSubmissionFailed(final TransientSubmission submission) {
        // retry
        processRetry(submission); 
        return submission;
    }

    private Submission onSubmissionCompleted(final TransientSubmission submission) {
        deregister(submission);
        success.inc();
        return submission;
    }
    
    private Submission onSubmissionDiscarded(final TransientSubmission submission, final Throwable error) {
        LOG.warn("discarding " + submission.getId() );
        deregister(submission);
        discarded.inc();
        throw Exceptions.propagate(error);
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
        return getAll().size();
    }
    
    public void register(TransientSubmission submission) {
        synchronized (this) {
            runningSubmissions.add(submission);
        }
    }
    
    public void deregister(TransientSubmission submission) {
        synchronized (this) {
            runningSubmissions.remove(submission);
        }
    }

    private ImmutableSet<TransientSubmission> getAll() {
        ImmutableSet<TransientSubmission> runnings;
        synchronized (this) {
            runnings = ImmutableSet.copyOf(runningSubmissions);
        }
        return runnings;
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