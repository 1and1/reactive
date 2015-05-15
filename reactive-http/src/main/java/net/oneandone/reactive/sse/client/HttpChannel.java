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
 * HttpChannel represents an underlying network connection 
 * 
 * @author grro
 *
 */
interface HttpChannel extends Closeable {
    
    /**
     * @return the channel id 
     */
    String getId();
    
    /**
     * @param data the data to write
     * @return the write future
     */
    CompletableFuture<Void> writeAsync(String data);
    
    /**
     * @return true, if the channel read is suspended
     */
    boolean isReadSuspended();
    
    /**
     * @param isSuspended true, if is suspended
     */
    void suspendRead(boolean isSuspended);
    
    /**
     * terminates the channel by killing the connection without sending a proper close sequence 
     */
    void terminate();
    
    /**
     * closing the channel regularly
     */
    void close();
    
    /**
     * @return true, if the channel is open
     */
    boolean isOpen();
    
    
    
    
    /**
     * Do nothing channel
     * 
     * @author grro
     *
     */
    static class NullHttpChannel implements HttpChannel {
        
        private final AtomicBoolean isReadSuspended = new AtomicBoolean(false);
        
        /**
         * constructor
         * @param suspended true, if the stream is suspended
         */
        public NullHttpChannel(boolean suspended) {
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
        public void suspendRead(boolean isSuspended) {
            isReadSuspended.set(isSuspended);
        }
        
        @Override
        public void terminate() {
        }
        
        @Override
        public boolean isOpen() {
           return false;
        }
    }
}