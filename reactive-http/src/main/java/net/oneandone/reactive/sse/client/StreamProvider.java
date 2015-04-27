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
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;



interface StreamProvider extends Closeable {
    
    InboundStream newInboundStream(URI uri, Consumer<ByteBuffer[]> dataConsumer, Consumer<Throwable> errorConsumer);
    
    OutboundStream newOutboundStream(URI uri, Consumer<Void> closeConsumer);

    @Override
    public void close();
    
    


    static interface OutboundStream extends Closeable  {
        
        CompletableFuture<Void> write(String msg);
        
        void terminate();  
     
        void close();
    }
    
    
    static interface InboundStream extends Closeable {
        
        boolean isSuspended();
        
        void suspend();
        
        void resume();
        
        void terminate();
        
        void close();
    }
}        
    


    

