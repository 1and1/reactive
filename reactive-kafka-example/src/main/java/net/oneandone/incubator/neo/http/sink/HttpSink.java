/*
 * Copyright 1&1 Internet AG, htt;ps://github.com/1and1/
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
import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;



public interface HttpSink extends BiConsumer<Object, String>, Closeable {
    
    public static Method DEFAULT_METHOD = Method.POST;
    public static int DEFAULT_BUFFERSIZE = Integer.MAX_VALUE;
    public static File DEFAULT_PERSISTENCY_DIR = null;
    public static ImmutableSet<Integer> DEFAULT_REJECTSTATUS_LIST = ImmutableSet.of(400, 403, 405, 406, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417);
    public static ImmutableList<Duration> DEFAULT_RETRY_PAUSES = ImmutableList.of();
    public static int DEFAULT_PARALLELITY = 1;
    
    public enum Method {
        POST, PUT
    };
    
    
    @Override
    default void accept(Object entity, String mediaType) {
        submit(entity, mediaType);
    }

    default Submission submit(Object entity, String mediaType) {
        try {
            return submitAsync(entity, mediaType).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t; 
            } else {
                throw new RuntimeException(t);
            }
        }
    }
    
    CompletableFuture<Submission> submitAsync(Object entity, String mediaType);
    
    Metrics getMetrics();

    boolean isOpen();
    
    @Override
    void close();
    
    public interface Submission {
        
        public enum State { PENDING, COMPLETED, DISCARDED } 
        
        State getState();
    }
    

    static HttpSinkBuilder target(final String target) {
        return target(URI.create(target));
    }

    
    static HttpSinkBuilder target(final URI target) {
        Preconditions.checkNotNull(target);
        return new HttpSinkBuilderImpl(null, 
                                       target, 
                                       DEFAULT_METHOD, 
                                       DEFAULT_BUFFERSIZE,
                                       DEFAULT_PERSISTENCY_DIR,
                                       DEFAULT_REJECTSTATUS_LIST,
                                       DEFAULT_RETRY_PAUSES,
                                       DEFAULT_PARALLELITY);
    }   
}
