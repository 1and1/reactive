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

import javax.ws.rs.client.Client;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;



/**
 * the http sink provides a consumer-styled API for on-way HTTP upload
 * transactions. By performing such transaction data will be send to the server
 * without getting data back. The http sink provides auto-retries. <br>
 * <br>
 * 
 * Example with in-memory auto-retry
 * 
 * <pre>
 * EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics")
 *                               .withRetryAfter(Duration.ofMillis(100), 
 *                                               Duration.ofSeconds(3), 
 *                                               Duration.ofMinutes(1), 
 *                                               Duration.ofMinutes(30),
 *                                               Duration.ofHours(5))
 *                               .open();
 * 
 *  // tries to send data. If fails (5xx) it will be auto-retried. In case of 
 *  // an 4xx an BadRequestException will be thrown 
 *  sink.submitAsync(newCustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json"); 
 *  // ...
 *  
 *  sink.close();
 * <pre>
 * <br>
 * <br>
 * 
 * Example with persistent file-based memory auto-retry
 * 
 * <pre>
 * EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics")
 *                               .withRetryAfter(Duration.ofMillis(100),
 *                                               Duration.ofSeconds(3), 
 *                                               Duration.ofMinutes(1),
 *                                               Duration.ofMinutes(30),
 *                                               Duration.ofHours(5))
 *                               .withRetryPersistency(myRetryDir) 
 *                               .open();
 * 
 * // tries to send data. If fails (5xx) it will be auto-retried. In case of an
 * // 4xx an BadRequestException will be thrown 
 * sink.submitAsync(newCustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
 * // ...
 * 
 * sink.close();
 * <pre>
 *
 */
public interface HttpSink {
    public static Method DEFAULT_METHOD = Method.POST;
    public static int DEFAULT_BUFFERSIZE = Integer.MAX_VALUE;
    public static File DEFAULT_PERSISTENCY_DIR = null;
    public static ImmutableSet<Integer> DEFAULT_REJECTSTATUS_LIST = ImmutableSet.of(400, 403, 405, 406, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417);
    public static ImmutableList<Duration> DEFAULT_RETRY_PAUSES = ImmutableList.of();
    public static int DEFAULT_PARALLELITY = 1;
    
    public enum Method {
        POST, PUT
    };

    
    /**
     * @param target the target uri
     * @return a new instance of the http sink
     */
    static HttpSink create(final String target) {
        Preconditions.checkNotNull(target);
        return create(URI.create(target));
    }

    /**
     * @param target the target uri
     * @return a new instance of the http sink
     */
    static HttpSink create(final URI target) {
        Preconditions.checkNotNull(target);
        return new HttpSinkImpl(null, 
                                target, 
                                DEFAULT_METHOD, 
                                DEFAULT_BUFFERSIZE,
                                DEFAULT_PERSISTENCY_DIR,
                                DEFAULT_REJECTSTATUS_LIST,
                                DEFAULT_RETRY_PAUSES,
                                DEFAULT_PARALLELITY);
    }

    /**
     * @param client the client to use
     * @return a new instance of the http sink
     */
    HttpSink withClient(final Client client);

    /**
     * @param method the method. Supported are POST and PUT (default is {@link HttpSink#DEFAULT_METHOD})
     * @return a new instance of the http sink
     */
    HttpSink withMethod(final Method method);
    
    /**
     * @param retryPauses the delays before retrying (default is {@link HttpSink#DEFAULT_RETRY_PAUSES})
     * @return a new instance of the http sink
     */
    HttpSink withRetryAfter(final ImmutableList<Duration> retryPauses);
    
    /**
     * @param retryPauses the delays before retrying (default is {@link HttpSink#DEFAULT_RETRY_PAUSES})
     * @return a new instance of the http sink
     */
    HttpSink withRetryAfter(final Duration... retryPauses);
    

    /**
     * @param numParallelWorkers the parallelity by performing retries (default is {@link HttpSink#DEFAULT_PARALLELITY})
     * @return a new instance of the http sink
     */
    HttpSink withRetryParallelity(final int numParallelWorkers);
    
    /**
     * @param bufferSize the retry buffer size. If the size is exceeded, new retry jobs will be discarded (default is {@link HttpSink#DEFAULT_BUFFERSIZE})
     * @return a new instance of the http sink
     */
    HttpSink withRetryBufferSize(final int bufferSize);
    
    /**
     * @param dir the directory where the retry jobs will be stored. If null,
     *            the retry jobs will be stored in-memory (default is {@link HttpSink#DEFAULT_PERSISTENCY_DIR})
     * @return a new instance of the http sink
     */
    HttpSink withRetryPersistency(final File dir);
    
    /**
     * 
     * @param rejectStatusList the set of status codes which will not initiate a retry. Instead a runtime exception will be 
     *                         thrown, if such a response status is received (default {@link HttpSink#DEFAULT_REJECTSTATUS_LIST})
     * @return a new instance of the http sink
     */
    HttpSink withRejectOnStatus(final ImmutableSet<Integer> rejectStatusList);
    
    /**
     * @return the sink reference
     */
    EntityConsumer open();
}    

