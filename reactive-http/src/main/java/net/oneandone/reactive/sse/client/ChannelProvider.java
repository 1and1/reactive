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



import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpResponse;

import java.io.Closeable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.ImmutableMap;


interface ChannelProvider {
    
    CompletableFuture<Stream> openChannelAsync(String id,
                                              URI uri, 
                                              String method, 
                                              ImmutableMap<String, String> headers, 
                                              boolean isFailOnConnectError,
                                              int numFollowRedirects,
                                              ChannelHandler handler,
                                              Optional<Duration> connectTimeout);
    
    
    CompletableFuture<Void> closeAsync();
    

    
    static interface Stream extends Closeable {
        
        CompletableFuture<Void> writeAsync(String data);
        
        boolean isReadSuspended();
        
        void suspendRead();
        
        void resumeRead();
        
        void terminate();
        
        void close();
        
        boolean isConnected();
    }
    
    
    static interface ChannelHandler {
        
        default ChannelHandler onResponseHeader(Channel channel, HttpResponse response) {
            onError(channel.hashCode(), new IllegalStateException("got unexpected response header"));
            return new ChannelHandler() { };
        }
            
        default Optional<ChannelHandler> onContent(int channelId, ByteBuffer[] buffers) {
            return Optional.of(new ChannelHandler() { });
        }
        
        default void onError(int channelId, Throwable error) {  }
     }

    
    static class NullChannel implements Stream {
        
        @Override
        public void close() {
        }
        
        @Override
        public CompletableFuture<Void> writeAsync(String data) {
            return CompletableFuture.completedFuture(null);
        }
        
        @Override
        public boolean isReadSuspended() {
            return false;
        }
        
        @Override
        public void resumeRead() {
        }
        
        @Override
        public void suspendRead() {
        }
        
        @Override
        public void terminate() {
        }
        
        @Override
        public boolean isConnected() {
           return false;
        }
    }
}        
    


    

