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


import io.netty.handler.codec.http.HttpHeaders;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import net.oneandone.reactive.ConnectException;
import net.oneandone.reactive.ReactiveSource;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.ServerSentEventParser;
import net.oneandone.reactive.sse.client.HttpChannelDataHandler;
import net.oneandone.reactive.utils.Immutables;
import net.oneandone.reactive.utils.Utils;
import net.oneandone.reactive.utils.SubscriberNotifier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Queues;




/**
 * A reactive client endpoint to consume data from a Server-Sent Event stream 
 *  
 * @author grro
 */
public class ClientSseSource implements Publisher<ServerSentEvent> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ClientSseSource.class);
    private static final int DEFAULT_NUM_FOILLOW_REDIRECTS = 9;
    private static final int DEFAULT_BUFFER_SIZE = 50;

    // properties
    private final URI uri;
    private final boolean isFailOnConnectError;
    private final Optional<String> lastEventId;
    private final Optional<Duration> connectionTimeout;
    private final int numPrefetchedElements;
    private final int numFollowRedirects;
    

    private ClientSseSource(URI uri,
                            Optional<String> lastEventId,
                            boolean isFailOnConnectError,
                            int numFollowRedirects,
                            int numPrefetchedElements,
                            Optional<Duration> connectionTimeout) {
        this.uri = uri;
        this.lastEventId = lastEventId;
        this.isFailOnConnectError = isFailOnConnectError;
        this.numFollowRedirects = numFollowRedirects;
        this.numPrefetchedElements = numPrefetchedElements;
        this.connectionTimeout = connectionTimeout;
    }
    
    
    
    /**
     * constructor
     * @param uri  the uri 
     */
    public ClientSseSource(String uri) {
        this(URI.create(uri));
    }
    
    
    /**
     * constructor
     * @param uri  the uri 
     */
    public ClientSseSource(URI uri) {
        this(uri, 
             Optional.empty(), 
             true, 
             DEFAULT_NUM_FOILLOW_REDIRECTS,
             DEFAULT_BUFFER_SIZE,
             Optional.empty());
    }
    

    /**
     * @param connectionTimeout the connection timeout to use
     * @return a new instance with the updated behavior 
     */
    public ClientSseSource connectionTimeout(Duration connectionTimeout) {
        return new ClientSseSource(this.uri, 
                                   this.lastEventId, 
                                   this.isFailOnConnectError,
                                   this.numFollowRedirects,
                                   this.numPrefetchedElements,
                                   Optional.of(connectionTimeout));
    }

 

    /**
     * @param lastEventId the last seen event id 
     * @return a new instance with the updated behavior
     */
    public ClientSseSource withLastEventId(String lastEventId) {
        return new ClientSseSource(this.uri,
                                   Optional.ofNullable(lastEventId),
                                   this.isFailOnConnectError,
                                   this.numFollowRedirects,
                                   this.numPrefetchedElements,
                                   this.connectionTimeout);
    }

    
    /**
     * @param isFailOnConnectError true, if connect should fail on connect errors
     * @return a new instance with the updated behavior
     */
    public ClientSseSource failOnConnectError(boolean isFailOnConnectError) {
        return new ClientSseSource(this.uri,
                                   this.lastEventId,
                                   isFailOnConnectError,
                                   this.numFollowRedirects,
                                   this.numPrefetchedElements,
                                   this.connectionTimeout);
    }

    
    /**
     * @param isFollowRedirects true, if redirects should be followed
     * @return a new instance with the updated behavior
     */
    public ClientSseSource followRedirects(boolean isFollowRedirects) {
        return new ClientSseSource(this.uri,
                                   this.lastEventId,
                                   this.isFailOnConnectError,
                                   isFollowRedirects ? DEFAULT_NUM_FOILLOW_REDIRECTS : 0,
                                   this.numPrefetchedElements,
                                   this.connectionTimeout);
    }


    /**
     * @param numPrefetchedElements  the size of the internal receive buffer
     * @return a new instance with the updated behavior
     */
    public ClientSseSource buffer(int numPrefetchedElements) {
        return new ClientSseSource(this.uri,
                                   this.lastEventId,
                                   this.isFailOnConnectError,
                                   this.numFollowRedirects,
                                   numPrefetchedElements,
                                   this.connectionTimeout);
    }

    
    /**
     * @return the new source instance
     * @throws ConnectException if an connect error occurs
     */
    public ReactiveSource<ServerSentEvent> open() throws ConnectException {
        return Utils.get(openAsync());
    }

    
    /**
     * @return the new source instance future
     */
    public CompletableFuture<ReactiveSource<ServerSentEvent>> openAsync() {
        return ReactiveSource.subscribeAsync(this);
    }
    
    
    
    @Override
    public void subscribe(Subscriber<? super ServerSentEvent> subscriber) {
        // https://github.com/reactive-streams/reactive-streams-jvm#1.9
        if (subscriber == null) {  
            throw new NullPointerException("subscriber is null");
        }

        
        new SseInboundStreamSubscription(uri,
                                         lastEventId,
                                         isFailOnConnectError,
                                         numFollowRedirects,
                                         numPrefetchedElements,
                                         connectionTimeout,
                                         subscriber);
    }
    
    
    
    /**
     * internal subscription handle
     * 
     * @author grro
     */
    private static class SseInboundStreamSubscription implements Subscription {
        private final String id = "cl-in-" + UUID.randomUUID().toString();
        private final AtomicBoolean isOpen = new AtomicBoolean(true);

        // underlying stream
        private final ReconnectingHttpChannel sseConnection;

        // incoming event handling
        private final FlowControl flowControl;
        private final InboundBuffer inboundBuffer;
        private final SubscriberNotifier<ServerSentEvent> subscriberNotifier; 

        
    
        private SseInboundStreamSubscription(URI uri, 
                                             Optional<String> lastId,
                                             boolean isFailOnConnectError,
                                             int numFollowRedirects,
                                             int numPrefetchedElements,
                                             Optional<Duration> connectionTimeout, 
                                             Subscriber<? super ServerSentEvent> subscriber) {

            this.subscriberNotifier = new SubscriberNotifier<>(subscriber, this);

            this.flowControl = new FlowControl(numPrefetchedElements);
            this.inboundBuffer = new InboundBuffer(id, 
                                                   flowControl,
                                                   (event) -> subscriberNotifier.notifyOnNext(event), 
                                                   lastId);
            
            sseConnection = new ReconnectingHttpChannel(id, 
                                                        uri,
                                                        "GET", 
                                                        ImmutableMap.of(HttpHeaders.Names.ACCEPT, "text/event-stream"),
                                                        isFailOnConnectError, 
                                                        numFollowRedirects, 
                                                        connectionTimeout, 
                                                        (stream) -> { },                          // connect listener
                                                        inboundBuffer,                           // data consumer
                                                        (headers) -> Immutables.join(headers, inboundBuffer.getLastEventId().map(id -> ImmutableMap.of("Last-Event-ID", id)).orElse(ImmutableMap.of())));
            
            sseConnection.init()
                         .thenAccept(isConnected -> subscriberNotifier.start())
                         .exceptionally(error -> { subscriberNotifier.start(error); return null; });
        }
 
     
        
        @Override
        public void cancel() {
            sseConnection.close();
        } 
       
        
        
        
        @Override
        public void request(long n) {
            if (isOpen.get()) {
                if(n < 0) {
                    // https://github.com/reactive-streams/reactive-streams#3.9
                    subscriberNotifier.notifyOnError((new IllegalArgumentException("Non-negative number of elements must be requested: https://github.com/reactive-streams/reactive-streams#3.9")));
                } else {
                    inboundBuffer.onRequested((int) n);
                }
            } else {
                subscriberNotifier.notifyOnError((new IllegalArgumentException("source is closed")));
            }
        }


         
        @Override
        public String toString() {
           return sseConnection.toString() + "  buffered events: " + inboundBuffer.getNumBuffered() + ", num requested: " + inboundBuffer.getNumPendingRequests();
        }
     
        
        
        private final class FlowControl implements InboundBufferListener {
            private final int numPrefetchedElements;

            public FlowControl(int numPrefetchedElements) {
                this.numPrefetchedElements = numPrefetchedElements;
            }
   
            public void onElementAdded(int numBuffered, int numPendingRequest) {
                // [Flow-control] will be suspended, if num pre-fetched more than requested ones
                if ((numBuffered > getMaxBuffersize(numPendingRequest)) && !sseConnection.isReadSuspended()) {
                    sseConnection.suspendRead(true);
                }
            }

            public void onElementRemoved(int numBuffered, int numPendingRequest) {
                // [Flow-control] will be resumed, if num pre-fetched less than one or 25% of the max buffer size
                if (sseConnection.isReadSuspended() && ( (numBuffered < 1) || (numBuffered < getMaxBuffersize(numPendingRequest) * 0.25)) ) {
                    sseConnection.suspendRead(false);
                }
            }

            private int getMaxBuffersize(int numPendingRequest) {
                return numPendingRequest + numPrefetchedElements;
            }
        }

        
        


        private static interface InboundBufferListener {
            
            void onElementAdded(int numBuffered, int numPendingRequest);

            void onElementRemoved(int numBuffered, int numPendingRequest);
        }

        
        
        
        private static class InboundBuffer implements HttpChannelDataHandler {
            private final Queue<ServerSentEvent> bufferedEvents = Queues.newConcurrentLinkedQueue();
            private final ServerSentEventParser parser = new ServerSentEventParser();
            
            private final String id;
            private final InboundBufferListener elementListener;
            private final Consumer<ServerSentEvent> eventConsumer;

            private final AtomicInteger numPendingRequested = new AtomicInteger(0);
            private Optional<String> lastEventId;

            
            
            public InboundBuffer(String id, InboundBufferListener elementListener, Consumer<ServerSentEvent> eventConsumer, Optional<String> lastEventId) {
                this.id = id;
                this.elementListener = elementListener;
                this.eventConsumer = eventConsumer;
                this.lastEventId = lastEventId;
            }
            

            public void onRequested(int num) {
                numPendingRequested.addAndGet(num);
                process();
            }
            

            @Override
            public void onContent(String streamlId, ByteBuffer[] data) {
    
                for (int i = 0; i < data.length; i++) {
                
                    for (ServerSentEvent event : parser.parse(data[i])) {
                        logEventReceived(event);
                        
                        if (!event.isSystem()) {
                            bufferedEvents.offer(event);
                            lastEventId = event.getId();
                            elementListener.onElementAdded(bufferedEvents.size(), numPendingRequested.get());
                        }
                    }
                }
                
                process();
            }

            
            private void process() {
                while (numPendingRequested.get() > 0) {
                    ServerSentEvent event = bufferedEvents.poll();
                    if (event == null) {
                        return;
                    } else {
                        eventConsumer.accept(event);
                        numPendingRequested.decrementAndGet();
                        elementListener.onElementRemoved(bufferedEvents.size(), numPendingRequested.get());
                    }
                }
            }
            

            @Override
            public void onError(String channelId, Throwable error) {
                LOG.debug("[" + id + "] resetting sse parser");
                parser.reset();
            }

            
            public synchronized Optional<String> getLastEventId() {
                return lastEventId;
            }
            
            public synchronized int getNumPendingRequests() {
                return numPendingRequested.get();
            }
            
            public synchronized int getNumBuffered() {
                return bufferedEvents.size();
            }
            
            private void logEventReceived(ServerSentEvent event) {
                if (LOG.isDebugEnabled()) {
                    String eventStr = event.toString().trim().replace("\r\n", "\\r\\n");
                    eventStr = (eventStr.length() > 100) ? (eventStr.substring(0, 100) + "...") : eventStr;
                    
                    if (event.isSystem()) {
                        LOG.debug("[" + id + "] system event received " + eventStr);
                    } else {
                        LOG.debug("[" + id + "] event received " + eventStr);
                    }
                }
            }
        }
    }
}  