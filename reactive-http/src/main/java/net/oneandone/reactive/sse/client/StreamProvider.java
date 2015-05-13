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
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;


interface StreamProvider {
    
    CompletableFuture<Stream> openStreamAsync(String id,
                                              URI uri, 
                                              String method, 
                                              ImmutableMap<String, String> headers, 
                                              boolean isFailOnConnectError,
                                              int numFollowRedirects,
                                              StreamHandler handler,
                                              Optional<Duration> connectTimeout);
    
    
    CompletableFuture<Void> closeAsync();
    

    
    static interface Stream extends Closeable {
        
        String getStreamId();
        
        CompletableFuture<Void> writeAsync(String data);
        
        boolean isReadSuspended();
        
        void suspendRead();
        
        void resumeRead();
        
        void terminate();
        
        void close();
        
        boolean isConnected();
    }
 
    
    
    static interface DataConsumer<T extends DataConsumer<?>> {
        
        default void onError(String channelId, Throwable error) { };
        
        @SuppressWarnings("unchecked")
        default Optional<T> onContent(String channelId, ByteBuffer[] data) { 
            return Optional.of((T) this);
        }
    }
    
    
    static interface StreamHandler extends DataConsumer<StreamHandler> {
        
        default StreamHandler onResponseHeader(String channelId, Channel channel, HttpResponse response) {
            onError(channelId, new IllegalStateException("got unexpected response header"));
            return new StreamHandler() { };
        }
            
        default Optional<StreamHandler> onContent(String channelId, ByteBuffer[] buffers) {
            return Optional.of(new StreamHandler() { });
        }
        
        default void onError(String channelId, Throwable error) {  }
     }

    
    static class NullStream implements Stream {
        
        private final AtomicBoolean isReadSuspended = new AtomicBoolean(false);
        
        public NullStream(boolean suspended) {
            isReadSuspended.set(suspended);
        }
        
        
        @Override
        public void close() {
        }
        
        @Override
        public String getStreamId() {
            return "<null>";
        }
        
        @Override
        public CompletableFuture<Void> writeAsync(String data) {
            CompletableFuture<Void> promise = new CompletableFuture<Void>();
            promise.completeExceptionally(new IllegalStateException("stream is disconnected"));
            return promise;
        }
        
        @Override
        public boolean isReadSuspended() {
            return isReadSuspended.get();
        }
        
        @Override
        public void resumeRead() {
            isReadSuspended.set(false);
        }
        
        @Override
        public void suspendRead() {
            isReadSuspended.set(true);
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
    


    

