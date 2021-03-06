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

import javax.ws.rs.core.MediaType;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


/**
 * A HttpSink represents the HTTP endpoint to push messages 
 *
 */
public interface HttpSink extends BiConsumer<Object, MediaType>, Closeable {    
    public static Method DEFAULT_METHOD = Method.POST;
    public static int DEFAULT_BUFFERSIZE = Integer.MAX_VALUE;
    public static File DEFAULT_PERSISTENCY_DIR = null;
    public static ImmutableList<Duration> DEFAULT_RETRY_PAUSES = ImmutableList.of();
    public static ImmutableSet<Integer> DEFAULT_REJECTSTATUS_LIST = ImmutableSet.of(400, 
    																				403, 
    																				405, 
    																				406, 
    																				408, 
    																				409, 
    																				410, 
    																				411,
    																				412, 
    																				413, 
    																				414, 
    																				415, 
    																				416,
    																				417);
    
    /**
     * HTTP Method enum
     */
    public enum Method {
        POST, PUT
    };
    
    /**
     * emits a message into the sink 
     * @param entity       the entity
     * @param mediaType    the media type
     */
    default void accept(final Object entity, final String mediaType) {
        accept(entity, MediaType.valueOf(mediaType));
    }

    /**
     * emits a message into the sink 
     * @param entity       the entity
     * @param mediaType    the media type
     */
    @Override
    default void accept(final Object entity, final MediaType mediaType) {
        submit(entity, mediaType);
    }
    
    /**
     * emits a message into the sink
     *  
     * @param entity      the entity 
     * @param mediaType   the media type of the entity
     * @return the submission
     */
    default Submission submit(final Object entity, final String mediaType) {
        return submit(entity, MediaType.valueOf(mediaType));
    }
    
    /**
     * emits a message into the sink
     *  
     * @param entity      the entity 
     * @param mediaType   the media type of the entity
     * @return the submission
     */
    default Submission submit(final Object entity, final MediaType mediaType) {
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
    
    /**
     * emits a message in an async way
     * 
     * @param entity      the entity
     * @param mediaType   the media type of the entity
     * @return the submission future
     */
    default CompletableFuture<Submission> submitAsync(final Object entity, final String mediaType) {
        return submitAsync(entity, MediaType.valueOf(mediaType));
    }
    
    /**
     * emits a message in an async way
     * 
     * @param entity      the entity to submit
     * @param mediaType   the media type of the entity
     * @return the submission future
     */
    CompletableFuture<Submission> submitAsync(final Object entity, final MediaType mediaType);
    
    /**
     * retrieves the metrics
     * @return the metrics
     */
    Metrics getMetrics();
    
    /**
     * @return the pending submissions
     */
    ImmutableSet<Submission> getPendingSubmissions();

    /**
     * @return true, if the sink is open
     */
    boolean isOpen();
    
    @Override
    void close();
    
    /**
     * creates a new sink builder
     * @param target the target uri
     * @return the builder
     */
    static HttpSinkBuilder target(final String target) {
        return target(URI.create(target));
    }

    /**
     * creates a new sink builder
     * @param target the target uri
     * @return the builder
     */
    static HttpSinkBuilder target(final URI target) {
        Preconditions.checkNotNull(target);
        return new HttpSinkBuilderImpl(null, 
                                       target, 
                                       DEFAULT_METHOD, 
                                       DEFAULT_BUFFERSIZE,
                                       DEFAULT_PERSISTENCY_DIR,
                                       DEFAULT_REJECTSTATUS_LIST,
                                       DEFAULT_RETRY_PAUSES);
    }   

    
    /**
     * The metrics
     */
    public interface Metrics {
        
    	/**
    	 * @return num success
    	 */
        Counter getNumSuccess();

        /**
         * @return num retries
         */
        Counter getNumRetries();

        /**
         * @return num discarded
         */
        Counter getNumDiscarded();
    }
}