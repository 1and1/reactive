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
package net.oneandone.reactive.sse.servlet;



import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;

import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.ServerSentEventParser;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;



 
class SseReadableChannel {
    private final Object pendingConsumesLock = new Object();
    private final AtomicInteger numPendingConsumes = new AtomicInteger(0);

    private final SSEInputStream serverSentEventsStream;
    
    private final Consumer<ServerSentEvent> eventConsumer;
    private final Consumer<Throwable> errorConsumer;
    private final Consumer<Void> completionConsumer;
    

    
    public SseReadableChannel(ServletInputStream in, Consumer<ServerSentEvent> eventConsumer, Consumer<Throwable> errorConsumer, Consumer<Void> completionConsumer) {
        this.eventConsumer = eventConsumer;
        this.errorConsumer = errorConsumer;
        this.completionConsumer = completionConsumer;
        this.serverSentEventsStream = new SSEInputStream(in);
        
        in.setReadListener(new ServletReadListener());
    }

    
    private final class ServletReadListener implements ReadListener {
        
        @Override
        public void onAllDataRead() throws IOException {
            completionConsumer.accept(null);
        }
        
        @Override
        public void onError(Throwable t) {
            SseReadableChannel.this.onError(t);
        }
        
        @Override
        public void onDataAvailable() throws IOException {
            proccessPendingReads();
        }
    }

    
    
    /**
     * reads the 
     * @param consumer
     */
    public void consumeNextEvent() {
        
        synchronized (pendingConsumesLock) {   
            try {
                // reading has to be processed inside the sync block to avoid shuffling events 
                Optional<ServerSentEvent> optionalEvent = serverSentEventsStream.next();
                
                // got an event?
                if (optionalEvent.isPresent()) {
                    eventConsumer.accept(optionalEvent.get());
                 
                // no, queue the pending read request    
                // will be handled by performing the read listener's onDataAvailable callback     
                } else {
                    numPendingConsumes.incrementAndGet();
                }
                
            } catch (IOException | RuntimeException t) {
                onError(t);
            }
        }
    }
    
    
    private void proccessPendingReads() {
        
        synchronized (pendingConsumesLock) {
            try {
                while(numPendingConsumes.get() > 0) {
                    Optional<ServerSentEvent> optionalEvent = serverSentEventsStream.next();
                    if (optionalEvent.isPresent()) {
                        numPendingConsumes.decrementAndGet();
                        eventConsumer.accept(optionalEvent.get());
                    } else {
                        return;
                    }
                }
            } catch (IOException | RuntimeException t) {
                onError(t);
            }
        }
    }
    
    
    private void onError(Throwable t)  {
        errorConsumer.accept(t);
        close();
    }
    
    
    public void close() {
        serverSentEventsStream.close();
    }
    

    /**
     * SSEInputStream
     * 
     * @author grro
     */
    private static class SSEInputStream implements Closeable {
        
        // buffer
        private final Queue<ServerSentEvent> bufferedEvents = Lists.newLinkedList();
    
        // sse parser
        private final ServerSentEventParser parser = new ServerSentEventParser();
        private final byte buf[] = new byte[1024];
        private int len = -1;
        
        private final ServletInputStream is;
    
        
        public SSEInputStream(ServletInputStream is) {
            this.is = is;
        }
       
        
        public void close() {
            Closeables.closeQuietly(is);
        }
    
        
        public Optional<ServerSentEvent> next() throws IOException {
            
            // no events buffered        
            if (bufferedEvents.isEmpty()) {
                
                // read network
                while (bufferedEvents.isEmpty() && isNetworkdataAvailable() && (len = is.read(buf)) > 0) {            
                    parser.parse(ByteBuffer.wrap(buf, 0, len)).forEach(event -> bufferedEvents.add(event)); 
                }    
            }
              
            return Optional.ofNullable(bufferedEvents.poll());
        }
        
        private boolean isNetworkdataAvailable() {
            try {
                return is.isReady();
            } catch (IllegalStateException ise)  {
                return false;
            }
        }
    }
}    
