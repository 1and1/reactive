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
package net.oneandone.reactive.sse.client;


import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.ImmutableMap;


/**
 * HttpChannelProvider 
 * 
 * @author grro
 */
interface HttpChannelProvider {

    
    /**
     * closes the provider 
     * 
     * @return the close future
     */
    CompletableFuture<Void> closeAsync();
    
    
    
    /**
     * creates a new stream 
     *  
     * @param params the connection parameters
     * @return the Stream future
     */
    CompletableFuture<HttpChannel> newHttpChannelAsync(ConnectionParams params);
    
    
    
    /**
     * ConnectionParams
     * 
     * @author grro
     */
    static final class ConnectionParams {
        private final String id;
        private final URI uri;
        private final String method;
        private final ImmutableMap<String, String> headers; 
        private final int numFollowRedirects;
        private final HttpChannelDataHandler dataHandler;
        private final Optional<Duration> connectTimeout;
    
        /**
         * constructor 
         * @param id                      the channel base id 
         * @param uri                     the uri to connect
         * @param method                  the HTTP method to use
         * @param headers                 the additional headers
         * @param numFollowRedirects      the the max number of redirects will should be followed  
         * @param dataHandler             the data handler
         * @param connectTimeout          the connect timeout 
         */
        public ConnectionParams(String id,
                                URI uri, 
                                String method, 
                                ImmutableMap<String, String> headers, 
                                int numFollowRedirects,
                                HttpChannelDataHandler dataHandler,
                                Optional<Duration> connectTimeout) {
            this.id = id;
            this.uri = uri;
            this.method = method;
            this.headers = headers;
            this.numFollowRedirects = numFollowRedirects;
            this.dataHandler = dataHandler;
            this.connectTimeout = connectTimeout;
        }

        public String getId() {
            return id;
        }

        public URI getUri() {
            return uri;
        }

        public String getMethod() {
            return method;
        }

        public ImmutableMap<String, String> getHeaders() {
            return headers;
        }

        public int getNumFollowRedirects() {
            return numFollowRedirects;
        }

        public HttpChannelDataHandler getDataHandler() {
            return dataHandler;
        }

        public Optional<Duration> getConnectTimeout() {
            return connectTimeout;
        }
    }
}        