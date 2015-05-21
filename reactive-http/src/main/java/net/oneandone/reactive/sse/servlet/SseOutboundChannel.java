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

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.reactive.sse.ScheduledExceutor;
import net.oneandone.reactive.sse.ServerSentEvent;

import com.google.common.collect.Lists;





class SseOutboundChannel  {
    private static final Logger LOG = LoggerFactory.getLogger(SseOutboundChannel.class);
    
    private final static Duration DEFAULT_KEEP_ALIVE_PERIOD = Duration.ofSeconds(40); 

    private final String id = "srv-out-" + UUID.randomUUID().toString();
    
    private final Queue<Write> pendingWrites = Lists.newLinkedList();
    private final ServletOutputStream out;
    private final Consumer<Throwable> errorConsumer;


    
    public SseOutboundChannel(ServletOutputStream out, Consumer<Throwable> errorConsumer) {
        this(out, errorConsumer, DEFAULT_KEEP_ALIVE_PERIOD);
    }

    
    public SseOutboundChannel(ServletOutputStream out, Consumer<Throwable> errorConsumer, Duration keepAlivePeriod) {
        this.errorConsumer = errorConsumer;
        this.out = out;
        out.setWriteListener(new ServletWriteListener());
        
        // write http header, implicitly
        LOG.debug("[" + id + "] opened");
        
        // start the keep alive emitter 
        new KeepAliveEmitter(this, keepAlivePeriod).start();
    }

    
    public CompletableFuture<Integer> writeEventAsync(ServerSentEvent event) {       
        Write write = new Write(event);
        
        synchronized (pendingWrites) {
            pendingWrites.offer(write);
        }
        process();
        
        return write;
    }   
    
    
    private void process() {
        
        synchronized (pendingWrites) {
            while(isWritePossible() && !pendingWrites.isEmpty()) {
                Write write = pendingWrites.poll();
                write.perform();
            }
        }
    }
    
    
    private int writeToStream(ServerSentEvent event) {
        if (event.isSystem()) {
            LOG.debug("[" + id + "] writing system event " + event.toString().trim());
        } else {
            LOG.debug("[" + id + "] writing event " + event.getId().orElse(""));
        }
        
        try {
            synchronized (out) {
                byte[] data = event.toWire().getBytes("UTF-8");
                out.write(data);
                out.flush();
                
                return data.length;
            }
        } catch (IOException | RuntimeException t) {
            errorConsumer.accept(t);
            close();
            throw new RuntimeException(t);
        }
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

 
    
    public void close() {
        LOG.debug("[" + id + "] closing");
        try {
            writeEventAsync(ServerSentEvent.newEvent().comment("stop streaming"));
            out.close();
        } catch (IOException ignore) { }
    }
    
  
    
    
    private final class ServletWriteListener implements WriteListener {

        @Override
        public void onWritePossible() throws IOException {    
            process();
        }

        @Override
        public void onError(Throwable t) {
            errorConsumer.accept(t);
        }
    }  
    
    
    
    private final class Write extends CompletableFuture<Integer> {
        
        private final ServerSentEvent event;
        
        public Write(ServerSentEvent event) {
            this.event = event;
        }
        
        public void perform() {
            try {
                int written = writeToStream(event);
                complete(written);
            } catch (RuntimeException rt) {
                completeExceptionally(rt);
            }
        }
    }
    
  
    
    
    /**
     * sents keep alive messages to keep the http connection alive in case of idling
     * @author grro
     */
    private static final class KeepAliveEmitter {
        private final DecimalFormat formatter = new DecimalFormat("#.#");
        private final Instant start = Instant.now(); 
        
        private final ScheduledExecutorService executor = ScheduledExceutor.common();
        private final SseOutboundChannel channel;
        private final Duration keepAlivePeriod;
        
        
        public KeepAliveEmitter(SseOutboundChannel channel, Duration keepAlivePeriod) {
            this.channel = channel;
            this.keepAlivePeriod = keepAlivePeriod;
        }
        
        public void start() {
            writeAsync(ServerSentEvent.newEvent().comment("start server event streaming (keep alive period=" + keepAlivePeriod.getSeconds() + " sec)"));
            executor.schedule(() -> scheduleNextKeepAliveEvent(), (int) (keepAlivePeriod.getSeconds() * 0.5), TimeUnit.SECONDS);
        }
        
        private void scheduleNextKeepAliveEvent() {
            writeAsync(ServerSentEvent.newEvent().comment("keep alive from server (age " + format(Duration.between(start, Instant.now())) + ")"))
                    .thenAccept(numWritten -> executor.schedule(() -> scheduleNextKeepAliveEvent(), keepAlivePeriod.getSeconds(), TimeUnit.SECONDS));
        }     
        
        private CompletableFuture<Integer> writeAsync(ServerSentEvent event) {
            return channel.writeEventAsync(event);
        }
        
        private String format(Duration duration) {
            if (duration.getSeconds() > (60 * 60)) {
                return formatter.format(((float) duration.getSeconds() / (60 * 60))) + " hours";
            } else if (duration.getSeconds() > 120) {
                return formatter.format(((float) duration.getSeconds() / 60)) + " min";
            } else {
                return duration.getSeconds() + " sec";
            }
        }
    } 
}
