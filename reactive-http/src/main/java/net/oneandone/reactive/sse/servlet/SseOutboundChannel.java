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




import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;

import net.oneandone.reactive.sse.ServerSentEvent;

import com.google.common.collect.Lists;





class SseOutboundChannel  {
    private final static int DEFAULT_KEEP_ALIVE_PERIOD_SEC = 30; 

    private final List<CompletableFuture<Boolean>> whenWritePossibles = Lists.newArrayList();
    private final ServletOutputStream out;
    private final Consumer<Throwable> errorConsumer;


    
    public SseOutboundChannel(ServletOutputStream out, Consumer<Throwable> errorConsumer) {
        this(out, errorConsumer, Duration.ofSeconds(DEFAULT_KEEP_ALIVE_PERIOD_SEC));
    }

    
    public SseOutboundChannel(ServletOutputStream out, Consumer<Throwable> errorConsumer, Duration keepAlivePeriod) {
        this.errorConsumer = errorConsumer;
        this.out = out;
        out.setWriteListener(new ServletWriteListener());
        
            // start the keep alive emitter 
        new KeepAliveEmitter(this, keepAlivePeriod).start();

        // write http header, implicitly 
        requestWriteNotificationAsync().thenAccept(Void -> flush());
    }

    
    public CompletableFuture<Integer> writeEventAsync(ServerSentEvent event) {       
        CompletableFuture<Integer> writtenFuture = new CompletableFuture<>();
        requestWriteNotificationAsync().thenAccept(Void -> writeToWrite(event, writtenFuture));
        
        return writtenFuture;
    }   
    
    
    private void writeToWrite(ServerSentEvent event, CompletableFuture<Integer> writtenSizeFuture) {       
        try {
            synchronized (out) {
                byte[] data = event.toWire().getBytes("UTF-8");
                out.write(data);
                out.flush();
                
                writtenSizeFuture.complete(data.length);
            }
        } catch (IOException | RuntimeException t) {
            errorConsumer.accept(t);
            writtenSizeFuture.completeExceptionally(t);
            close();
        }
    }   
    

    private CompletableFuture<Boolean> requestWriteNotificationAsync() {
        CompletableFuture<Boolean> whenWritePossible = new CompletableFuture<>();

        synchronized (whenWritePossibles) {
            if (isWritePossible()) {
                whenWritePossible.complete(true);
            } else {
                // if not the WriteListener#onWritePossible will be called by the servlet container later
                whenWritePossibles.add(whenWritePossible);
            }
        }
        
        return whenWritePossible;
    }
    
    
    private boolean isWritePossible() {
        
        // triggers that write listener's onWritePossible will be called, if is possible to write data
        // According to the Servlet 3.1 spec the onWritePossible will be invoked if and only if isReady() 
        // method has been called and has returned false.
        //
        // Unfortunately the Servlet 3.1 spec left it open how many bytes can be written
        // Jetty for instance keeps a reference to the passed byte array and essentially owns it until the write is complete
        
        try {
            return out.isReady();
        } catch (IllegalStateException ise)  {
            return false;
        }
    }

    
    private void flush() {
        try {
            out.flush();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    
    
    public void close() {
        try {
            out.close();
        } catch (IOException ignore) { }
    }
    
  
    
    
    private final class ServletWriteListener implements WriteListener {

        @Override
        public void onWritePossible() throws IOException {    
            synchronized (whenWritePossibles) {
                whenWritePossibles.forEach(whenWritePossible -> whenWritePossible.complete(null));
                whenWritePossibles.clear();
            }
        }

        @Override
        public void onError(Throwable t) {
            errorConsumer.accept(t);
        }
    }  
    
    
    
    /**
     * sents keep alive messages to keep the http connection alive in case of idling
     * @author grro
     */
    private static final class KeepAliveEmitter {
        private static final ScheduledThreadPoolExecutor EXECUTOR = newScheduledThreadPoolExecutor();
        private final SseOutboundChannel channel;
        private final Duration keepAlivePeriod;

        private static ScheduledThreadPoolExecutor newScheduledThreadPoolExecutor() {
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(0);
            executor.setKeepAliveTime(DEFAULT_KEEP_ALIVE_PERIOD_SEC * 2, TimeUnit.SECONDS);
            executor.allowCoreThreadTimeOut(true);
            
            return executor;
        }
        
        
        public KeepAliveEmitter(SseOutboundChannel channel, Duration keepAlivePeriod) {
            this.channel = channel;
            this.keepAlivePeriod = keepAlivePeriod;
        }
        
        public void start() {
            scheduleNextKeepAliveEvent();
        }
        
        private void scheduleNextKeepAliveEvent() {
            channel.writeEventAsync(ServerSentEvent.newEvent().comment("keep alive"))
                   .thenAccept(numWritten -> EXECUTOR.schedule(() -> scheduleNextKeepAliveEvent(), keepAlivePeriod.getSeconds(), TimeUnit.SECONDS));
        }        
    } 
}
