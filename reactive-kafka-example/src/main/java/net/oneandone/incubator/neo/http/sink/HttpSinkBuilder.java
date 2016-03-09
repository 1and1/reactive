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


import java.io.File;
import java.time.Duration;

import javax.ws.rs.client.Client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import net.oneandone.incubator.neo.http.sink.HttpSink.Method;


public interface HttpSinkBuilder {
    
    /**
     * @param client the client to use
     * @return a new instance of the http sink
     */
    HttpSinkBuilder withClient(Client client);

    /**
     * @param method the method. Supported are POST and PUT (default is {@link HttpSink#DEFAULT_METHOD})
     * @return a new instance of the http sink
     */
    HttpSinkBuilder withMethod(Method method);
    
    /**
     * @param retryPauses the delays before retrying (default is {@link HttpSink#DEFAULT_RETRY_PAUSES})
     * @return a new instance of the http sink
     */
    HttpSinkBuilder withRetryAfter(ImmutableList<Duration> retryPauses);
    
    /**
     * @param retryPauses the delays before retrying (default is {@link HttpSink#DEFAULT_RETRY_PAUSES})
     * @return a new instance of the http sink
     */
    HttpSinkBuilder withRetryAfter(Duration... retryPauses);
    
    /**
     * @param maxBufferedSubmissions the retry buffer size. If the size is exceeded, new submissions will be discarded (default is {@link HttpSink#DEFAULT_BUFFERSIZE})
     * @return a new instance of the http sink
     */
    HttpSinkBuilder withRetryBufferSize(int maxBufferedSubmissions);
    
    /**
     * @param dir the directory where the pending submission to retry will be stored. If null,
     *            the retry jobs will be stored in-memory (default is {@link HttpSink#DEFAULT_PERSISTENCY_DIR})
     * @return a new instance of the http sink
     */
    HttpSinkBuilder withPersistency(File dir);

    /**
     * @param isPersistent true, if the sink is persistent. Per default pending submission to retry will be stored int othe user home dir 
     * @return a new instance of the http sink
     */
    HttpSinkBuilder withPersistency(boolean isPersistent);
    
    /**
     * 
     * @param rejectStatusList the set of status codes which will not initiate a retry. Instead a runtime exception will be 
     *                         thrown, if such a response status is received (default {@link HttpSink#DEFAULT_REJECTSTATUS_LIST})
     * @return a new instance of the http sink
     */
    HttpSinkBuilder withRejectOnStatus(ImmutableSet<Integer> rejectStatusList);
    
    /**
     * 
     * @param rejectStatusList the set of status codes which will not initiate a retry. Instead a runtime exception will be 
     *                         thrown, if such a response status is received (default {@link HttpSink#DEFAULT_REJECTSTATUS_LIST})
     * @return a new instance of the http sink
     */
    HttpSinkBuilder withRejectOnStatus(Integer... rejectStatusList);
    
    /**
     * @return the sink reference
     */
    HttpSink open();
}    