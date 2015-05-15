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

import java.util.concurrent.CompletableFuture;


/**
 * HttpChannelProviderFactory
 * 
 * @author grro
 */
class HttpChannelProviderFactory  {
    private static final HttpChannelProvider COMMON = new NettyBasedHttpChannelProvider();
    
    private HttpChannelProviderFactory() { }
    
    
    /**
     * @return a new stream provider
     */
    public static HttpChannelProvider newHttpChannelProvider() {
        return new HttpChannelProviderHandle();
    } 
    
    
    private static final class HttpChannelProviderHandle implements HttpChannelProvider {
        private final HttpChannelProvider delegate;
        
        
        public HttpChannelProviderHandle() {
            delegate = COMMON;
        }
        
        @Override
        public CompletableFuture<HttpChannel> newHttpChannelAsync(ConnectionParams params) {
            return delegate.newHttpChannelAsync(params);
        }
        
        @Override
        public CompletableFuture<Void> closeAsync() {
            return CompletableFuture.completedFuture(null);
        }
    }
}        