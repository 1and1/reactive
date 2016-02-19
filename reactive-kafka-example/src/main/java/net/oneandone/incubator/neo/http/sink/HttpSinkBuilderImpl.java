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
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import net.oneandone.incubator.neo.collect.Immutables;
import net.oneandone.incubator.neo.http.sink.HttpSink.Method;



final class HttpSinkBuilderImpl implements HttpSinkBuilder {
    private final URI target;
    private final Client userClient;
    private final Method method;
    private final int bufferSize;
    private final File dir;
    private final ImmutableSet<Integer> rejectStatusList;
    private final ImmutableList<Duration> retryDelays;
    private final int numParallelWorkers;

    
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
    public HttpSinkBuilder withRejectOnStatus(int... rejectStatusList) {
        Preconditions.checkNotNull(rejectStatusList);
        Set<Integer> set = Sets.newHashSet(); 
        for (int status : rejectStatusList) {
            set.add(status);
        }
        return withRejectOnStatus(ImmutableSet.copyOf(set));
    }
    
    @Override
    public HttpSink open() {
        // if dir is set, sink will run in persistent mode  
        return (dir == null) ? new TransientHttpSink() : new PersistentHttpSink();
    }   
    
    
    private class TransientHttpSink implements HttpSink {
        final Processor processor = new Processor(userClient, numParallelWorkers, bufferSize);
        
        @Override
        public boolean isOpen() {
            return processor.isOpen();
        }
        
        @Override
        public void close() {
            processor.close();
        }
        
        @Override
        public ImmutableSet<Submission> getPendingSubmissions() {
            return processor.getPendingSubmissions();
        }
        
        @Override
        public Metrics getMetrics() {
            return processor;
        }
        
        @Override
        public CompletableFuture<Submission> submitAsync(final Object entity, final MediaType mediaType) {
            return newSubmissionAsync(Entity.entity(entity, mediaType), UUID.randomUUID().toString())  // create a new submission
                    .thenCompose(submission -> processor.processAsync(submission))                     // process them an
                    .thenApply(s -> (Submission) s);                                                   // cast it
        }
    
        protected CompletableFuture<TransientSubmissionTask> newSubmissionAsync(final Entity<?> entity, final String id) {
            return TransientSubmissionTask.newPersistentSubmissionAsync(id, 
                                                                        target, 
                                                                        method, 
                                                                        entity,
                                                                        rejectStatusList,
                                                                        Immutables.join(Duration.ofMillis(0), retryDelays), // add first trial (which is not a retry)
                                                                        0,                                                  // no trials performed yet
                                                                        Instant.now());                                     // last trial time is now (time starting point is now)
        }
        
        @Override
        public String toString() {
            return method + " " + target + "\r\n" + getMetrics();
        }
    }
    
    private final class PersistentHttpSink extends TransientHttpSink {

        public PersistentHttpSink() {
            super();
            PersistentSubmissionTask.processOldQueryFiles(dir, method, target, processor);  // process old submissions (if exists)
        }

        @Override
        protected CompletableFuture<TransientSubmissionTask> newSubmissionAsync(final Entity<?> entity, final String id) {
            return PersistentSubmissionTask.newPersistentSubmissionAsync(id, 
                                                                         target, 
                                                                         method,   
                                                                         entity,
                                                                         rejectStatusList,
                                                                         Immutables.join(Duration.ofMillis(0), retryDelays),  // add first trial (which is not a retry)
                                                                         0,                                                   // no trials performed yet
                                                                         Instant.now(),                                       // last trial time is now (time starting point is now)
                                                                         dir)
                                       .thenApply(sub -> (TransientSubmissionTask) sub);
        }    
    } 
}