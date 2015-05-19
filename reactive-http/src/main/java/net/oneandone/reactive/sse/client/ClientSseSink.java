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



import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import net.oneandone.reactive.ReactiveSink;
import net.oneandone.reactive.ConnectException;
import net.oneandone.reactive.sse.ScheduledExceutor;
import net.oneandone.reactive.sse.ServerSentEvent;


import net.oneandone.reactive.sse.client.HttpChannel;
import net.oneandone.reactive.utils.Utils;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;




public class ClientSseSink implements Subscriber<ServerSentEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ClientSseSink.class);
    private static final int DEFAULT_NUM_FOILLOW_REDIRECTS = 9;
    private static final Duration DEFAULT_KEEP_ALIVE_PERIOD = Duration.ofSeconds(35);
    private static final int DEFAULT_BUFFER_SIZE = 50;
    
    
    // properties
    private final URI uri;
    private final boolean isAutoId;
    private final boolean isFailOnConnectError;
    private final Optional<Duration> connectionTimeout;
    private final int numFollowRedirects;
    private final Duration keepAlivePeriod;
    private final int numBufferedElements;
    private final boolean isAutoRetry;
    
    
    // stream
    private final AtomicReference<SseOutboundStream> sseOutboundStreamRef = new AtomicReference<>();
    

    
    public ClientSseSink(URI uri) {
        this(uri, 
             true,
             DEFAULT_NUM_FOILLOW_REDIRECTS,
             true,
             Optional.empty(), 
             DEFAULT_KEEP_ALIVE_PERIOD,
             DEFAULT_BUFFER_SIZE,
             true);
    }

    private ClientSseSink(URI uri, 
                          boolean isAutoId,
                          int numFollowRedirects,
                          boolean isFailOnConnectError,
                          Optional<Duration> connectionTimeout, 
                          Duration keepAlivePeriod,
                          int numBufferedElements,
                          boolean isAutoRetry) {
        this.uri = uri;
        this.isAutoId = isAutoId;
        this.numFollowRedirects = numFollowRedirects;
        this.isFailOnConnectError = isFailOnConnectError;
        this.connectionTimeout = connectionTimeout;
        this.keepAlivePeriod = keepAlivePeriod;
        this.numBufferedElements = numBufferedElements;
        this.isAutoRetry = isAutoRetry;
    }
    
    public ClientSseSink autoId(boolean isAutoId) {
        return new ClientSseSink(this.uri, 
                                 isAutoId,
                                 this.numFollowRedirects,
                                 this.isFailOnConnectError,
                                 this.connectionTimeout,
                                 this.keepAlivePeriod,
                                 this.numBufferedElements,
                                 this.isAutoRetry);
    }


    public ClientSseSink connectionTimeout(Duration connectionTimeout) {
        return new ClientSseSink(this.uri,
                                 this.isAutoId, 
                                 this.numFollowRedirects,
                                 this.isFailOnConnectError,
                                 Optional.of(connectionTimeout), 
                                 this.keepAlivePeriod,
                                 this.numBufferedElements,
                                 this.isAutoRetry);
    }

    /**
     * @param keepAlivePeriod  the keep alive period 
     * @return a new instance with the updated behavior
     */
    public ClientSseSink keepAlivePeriod(Duration keepAlivePeriod) {
        return new ClientSseSink(this.uri, 
                                 this.isAutoId,
                                 this.numFollowRedirects,
                                 this.isFailOnConnectError,
                                 this.connectionTimeout,
                                 keepAlivePeriod,
                                 this.numBufferedElements,
                                 this.isAutoRetry);
    }

    
    /**
     * @param isFailOnConnectError true, if connect should fail on connect errors
     * @return a new instance with the updated behavior
     */
    public ClientSseSink failOnConnectError(boolean isFailOnConnectError) {
        return new ClientSseSink(this.uri, 
                                 this.isAutoId,
                                 this.numFollowRedirects,
                                 isFailOnConnectError,
                                 this.connectionTimeout,
                                 this.keepAlivePeriod,
                                 this.numBufferedElements,
                                 this.isAutoRetry);
    }

    
    /**
     * @param isFollowRedirects true, if redirects should be followed
     * @return a new instance with the updated behavior
     */
    public ClientSseSink followRedirects(boolean isFollowRedirects) {
        return new ClientSseSink(this.uri, 
                                 this.isAutoId,
                                 isFollowRedirects ? DEFAULT_NUM_FOILLOW_REDIRECTS : 0,
                                 this.isFailOnConnectError,
                                 this.connectionTimeout,
                                 this.keepAlivePeriod,
                                 this.numBufferedElements,
                                 this.isAutoRetry);
    }
    
    
    /**
     * @param numBufferedElements the outgoing buffer size
     * @return a new instance with the updated behavior
     */
    public ClientSseSink buffer(int numBufferedElements) {
        return new ClientSseSink(this.uri, 
                                 this.isAutoId,
                                 this.numFollowRedirects,
                                 this.isFailOnConnectError,
                                 this.connectionTimeout,
                                 this.keepAlivePeriod,
                                 numBufferedElements,
                                 this.isAutoRetry);
    }

    
    
    /**
     * @param isAutoRetry if failed send activity should be retried
     * @return a new instance with the updated behavior
     */
    public ClientSseSink autoRetry(boolean isAutoRetry) {
        return new ClientSseSink(this.uri, 
                                 this.isAutoId,
                                 this.numFollowRedirects,
                                 this.isFailOnConnectError,
                                 this.connectionTimeout,
                                 this.keepAlivePeriod,
                                 this.numBufferedElements,
                                 isAutoRetry);
    }

    
    public ReactiveSink<ServerSentEvent> open() {
        return Utils.get(openAsync());
    }
    
    
    /**
     * @return the new source instance future
     */
    public CompletableFuture<ReactiveSink<ServerSentEvent>> openAsync() {
        return ReactiveSink.publishAsync(this)
                           .exceptionally(error -> { throw (error instanceof ConnectException) ? (ConnectException) error : new ConnectException(error); });
    }
    

    @Override
    public void onSubscribe(Subscription subscription) {
        if (sseOutboundStreamRef.get() == null) {
            sseOutboundStreamRef.set(new SseOutboundStream(subscription,
                                                           uri,
                                                           isAutoId,
                                                           numFollowRedirects,
                                                           isFailOnConnectError,
                                                           connectionTimeout, 
                                                           keepAlivePeriod,
                                                           numBufferedElements,
                                                           isAutoRetry));
        } else {
            throw new IllegalStateException("already subscribed");
        }
    }

    
    @Override
    public void onNext(ServerSentEvent event) {
        sseOutboundStreamRef.get().write(event);
    }
    
    @Override
    public void onError(Throwable t) {
        sseOutboundStreamRef.get().terminate(t);
    }
    
    @Override
    public void onComplete() {
        sseOutboundStreamRef.get().close();
    }
    
    
    @Override
    public String toString() {
        SseOutboundStream stream = sseOutboundStreamRef.get();
        return (stream == null) ? "<null>": stream.toString();
    }
    
  
    
    private static final class SseOutboundStream {
        private final String id = "cl-out-" + UUID.randomUUID().toString();
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        
        // properties
        private final boolean isAutoId;
        private final boolean isAutoRetry;
        private final Subscription subscription;

        // underlying buffer/stream
        private final OutboundBuffer outboundBuffer;
        private final ReconnectingHttpChannel sseConnection;
        
        // auto event id support
        private final String globalId = UID.newId();
        private final AtomicLong nextLocalId = new AtomicLong(1);

        // statistics
        private final AtomicLong numSent = new AtomicLong();
        private final AtomicLong numSendErrors = new AtomicLong();
        private final AtomicLong numResendTrials = new AtomicLong();
         
        

        
        public SseOutboundStream(Subscription subscription, 
                                 URI uri, 
                                 boolean isAutoId,
                                 int numFollowRedirects,
                                 boolean isFailOnConnectError,
                                 Optional<Duration> connectionTimeout,
                                 Duration keepAlivePeriod,
                                 int maxBufferSize,
                                 boolean isAutoRetry) {
            
            this.subscription = subscription;
            this.isAutoId = isAutoId;
            this.isAutoRetry = isAutoRetry;
            
            this.outboundBuffer = new OutboundBuffer(maxBufferSize); 
            
            sseConnection = new ReconnectingHttpChannel(id, 
                                                        uri,
                                                        "POST", 
                                                        ImmutableMap.of("Content-Type", "text/event-stream", "Transfer-Encoding", "chunked"),
                                                        isFailOnConnectError, 
                                                        numFollowRedirects, 
                                                        connectionTimeout, 
                                                        (isWriteable) -> outboundBuffer.refresh(),    // writeable changed listener
                                                        new HttpChannelDataHandler()  { },        // data consumer
                                                        (headers) -> headers);

            sseConnection.init()
                         .thenAccept(isConnected -> { new KeepAliveEmitter(id, keepAlivePeriod, sseConnection).start(); subscription.request(1); })
                         .exceptionally((error) -> terminate(error)); 
        }
        
        
        
        
        
        public void write(ServerSentEvent event) {
            if (!event.getId().isPresent() && isAutoId) {
                event = ServerSentEvent.newEvent()
                                       .id(globalId + "-" + nextLocalId.getAndIncrement())
                                       .event(event.getEvent().orElse(null))
                                       .data(event.getData().orElse(null))
                                       .retry(event.getRetry().orElse(null))
                                       .comment(event.getComment().orElse(null));
            }
            
            outboundBuffer.onData(event);
        }
        

        
        public Void terminate(Throwable t) {
            sseConnection.terminate();
            close();
            return null;
        }
        
        
        public void close() {
            if (isOpen.getAndSet(false)) {
                LOG.debug("[" + id + "] closing");

                sseConnection.close();
                subscription.cancel();
            }
        }

        
        @Override
        public String toString() {
           return  sseConnection.toString() + ", numSent: " + numSent.get() + ", numSendErrors: " + numSendErrors + ", numResendTrials: " + numResendTrials;
        }
        
        
        private static void logEventWritten(String id, ServerSentEvent event) {
            if (LOG.isDebugEnabled()) {
                String eventStr = event.toString().trim().replace("\r\n", "\\r\\n");
                eventStr = (eventStr.length() > 100) ? (eventStr.substring(0, 100) + "...") : eventStr;
                
                if (event.isSystem()) {
                    LOG.debug("[" + id + "] system event written " + eventStr);
                } else {
                    LOG.debug("[" + id + "] event written " + eventStr);
                }
            }
        }
        
        
        
        private final class OutboundBuffer {
            private final LinkedList<ServerSentEvent> bufferedEvents; 
            private final int maxBufferSize;

            public OutboundBuffer(int maxBufferSize) {
                this.maxBufferSize = maxBufferSize;
                this.bufferedEvents = Lists.newLinkedList(); 
            }
            
            
            public void refresh() {
                process();
            }
    
            public void onData(ServerSentEvent event) {
                synchronized (bufferedEvents) {
                    if (bufferedEvents.size() > maxBufferSize) {
                        throw new IllegalStateException("buffer limit " + maxBufferSize + " exceeded");
                    }
                    
                    bufferedEvents.add(event);
                }

                process();
            }

            
            private void process() {
                
                if (sseConnection.isOpen()) {
                    
                    ServerSentEvent event;
                    synchronized (bufferedEvents) {
                        event = bufferedEvents.poll();
                    }
                    
                    if (event == null) {
                        return;
                    }
                    
                    
                    sseConnection.writeAsync(event.toWire())
                                 .thenAccept((Void) -> {
                                                         logEventWritten(id, event);
                                                         numSent.incrementAndGet();
                                                         subscription.request(1); 
                                                         process(); 
                                                       })
                                 .exceptionally((error -> {
                                                             numSendErrors.incrementAndGet();
                                                             
                                                             if (isAutoRetry) {
                                                                 LOG.debug("[" + id + "] " + error.getMessage() + " error occured by writing event " + event.getId().orElse("") + " requeue event to send buffer");
                                                                 numResendTrials.incrementAndGet();
                                                                 synchronized (bufferedEvents) {
                                                                     bufferedEvents.addFirst(event);
                                                                 }
                                                                 
                                                             } else { 
                                                                 LOG.debug("[" + id + "] " + error.getMessage() + " error occured by writing event " + event.getId().orElse("") + " terminating sink");
                                                                 subscription.cancel();
                                                             }
                                                             
                                                             process();
                                                             return null;
                                                          }));                

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
            
            private final String id;
            private final Duration keepAlivePeriod;
            private final HttpChannel stream;
            
            
            public KeepAliveEmitter(String id, Duration keepAlivePeriod, HttpChannel stream) {
                this.id = id;
                this.keepAlivePeriod = keepAlivePeriod;
                this.stream = stream;
            }
            
            public void start() {
                writeAsync(ServerSentEvent.newEvent().comment("start client event streaming (keep alive period=" + keepAlivePeriod.getSeconds() + " sec)"));
                executor.schedule(() -> scheduleNextKeepAliveEvent(), keepAlivePeriod.getSeconds(), TimeUnit.SECONDS);
            }
            
            private void scheduleNextKeepAliveEvent() {
                writeAsync(ServerSentEvent.newEvent().comment("keep alive from client (age " + format(Duration.between(start, Instant.now())) + ")"))
                    .whenComplete((numWritten, error) -> executor.schedule(() -> scheduleNextKeepAliveEvent(), keepAlivePeriod.getSeconds(), TimeUnit.SECONDS));
            }       
            
            private CompletableFuture<Void> writeAsync(ServerSentEvent event) {
                return stream.writeAsync(event.toWire())
                             .thenAccept((Void) -> logEventWritten(id, event));
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
    
    
    
    private static class UID {
        
        public static String newId() {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                
                UUID uuid = UUID.randomUUID();
                dos.writeLong(uuid.getMostSignificantBits());
                dos.writeLong(uuid.getLeastSignificantBits());
                dos.flush();
                
                return BaseEncoding.base64Url().omitPadding().encode(baos.toByteArray());
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
    }
}