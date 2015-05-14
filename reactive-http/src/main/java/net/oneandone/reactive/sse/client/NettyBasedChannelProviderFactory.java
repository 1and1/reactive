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



class NettyBasedChannelProviderFactory  {
    private static final StreamProvider COMMON = new NettyBasedChannelProvider();
    
    
    public static StreamProvider newStreamProvider() {
        return new StreamProviderHandle();
    } 
    
    
    private static final class StreamProviderHandle implements StreamProvider {
        private final StreamProvider delegate;
        
        
        public StreamProviderHandle() {
            delegate = COMMON;
        }
        
        @Override
        public CompletableFuture<Stream> newStreamAsync(ConnectionParams params) {
            return delegate.newStreamAsync(params);
        }
        
        @Override
        public CompletableFuture<Void> closeAsync() {
            return CompletableFuture.completedFuture(null);
        }
    }
}        