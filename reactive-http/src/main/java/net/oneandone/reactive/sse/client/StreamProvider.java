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



import java.io.Closeable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;



interface StreamProvider extends Closeable {
    
    CompletableFuture<InboundStream> openInboundStreamAsync(String id,
                                                            URI uri, 
                                                            Optional<String> lastEventId, 
                                                            Consumer<ByteBuffer[]> dataConsumer, 
                                                            Consumer<Void> closeConsumer,
                                                            Consumer<Throwable> errorConsumer,
                                                            Optional<Duration> connectTimeout, 
                                                            Optional<Duration> socketTimeout);
    
    CompletableFuture<OutboundStream> newOutboundStream(String id, 
                                                        URI uri,
                                                        Consumer<Void> closeConsumer,
                                                        Optional<Duration> connectTimeout, 
                                                        Optional<Duration> socketTimeout);

    @Override
    public void close();
    
    


    static interface OutboundStream extends Closeable  {
        
        CompletableFuture<Void> write(String msg);
        
        void terminate();  
     
        void close();
    }
    
    static class EmptyOutboundStream implements OutboundStream {
        
        @Override
        public CompletableFuture<Void> write(String msg) {
            return CompletableFuture.completedFuture(null);
        }
        
        @Override
        public void close() {
        }

        @Override
        public void terminate() {
        }
    }
    
    static interface InboundStream extends Closeable {
        
        boolean isSuspended();
        
        void suspend();
        
        void resume();
        
        void terminate();
        
        void close();
    }
    
    static class EmptyInboundStream implements InboundStream {
        
        @Override
        public void close() {
        }
        
        @Override
        public boolean isSuspended() {
            return false;
        }
        
        @Override
        public void resume() {
        }
        
        @Override
        public void suspend() {
        }
        
        @Override
        public void terminate() {
        }
    }
}        
    


    

