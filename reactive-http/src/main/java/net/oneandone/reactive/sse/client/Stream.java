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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;




/**
 * Stream represents an underlying network connection 
 * @author grro
 *
 */
interface Stream extends Closeable {
    
    /**
     * @return the stream id 
     */
    String getId();
    
    /**
     * @param data the data to write
     * @return the write future
     */
    CompletableFuture<Void> writeAsync(String data);
    
    /**
     * @return true, if the stream read is suspended
     */
    boolean isReadSuspended();
    
    /**
     * suspend reading the stream
     */
    void suspendRead();

    /**
     * resume reading the stream
     */
    void resumeRead();
    
    /**
     * terminates the stream by killing the connection without sending a proper close sequence 
     */
    void terminate();
    
    /**
     * closing the stream regularly
     */
    void close();
    
    /**
     * @return true, if the stream is connected
     */
    boolean isConnected();
    
    
    
    
    /**
     * Do nothing stream
     * 
     * @author grro
     *
     */
    static class NullStream implements Stream {
        
        private final AtomicBoolean isReadSuspended = new AtomicBoolean(false);
        
        /**
         * constructor
         * @param suspended true, if the stream is suspended
         */
        public NullStream(boolean suspended) {
            isReadSuspended.set(suspended);
        }
        
        
        @Override
        public void close() {
        }
        
        @Override
        public String getId() {
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


    


    

