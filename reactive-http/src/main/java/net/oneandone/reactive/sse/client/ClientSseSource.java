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


import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import net.oneandone.reactive.ReactiveSource;
import net.oneandone.reactive.sse.ScheduledExceutor;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.ServerSentEventParser;
import net.oneandone.reactive.sse.client.StreamProvider.InboundStream;
import net.oneandone.reactive.sse.client.StreamProvider.InboundStreamHandler;
import net.oneandone.reactive.utils.SubscriberNotifier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;



/**
 * A reactive client endpoint to consume data from a Server-Sent Event stream 
 *  
 * @author grro
 */
public class ClientSseSource implements Publisher<ServerSentEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ClientSseSource.class);
    
    private final static int DEFAULT_BUFFER_SIZE = 50;
    private static final int DEFAULT_NUM_FOILLOW_REDIRECTS = 9;
    
    private final URI uri;
    private boolean isFailOnConnectError;
    private final Optional<String> lastEventId;
    private final Optional<Duration> connectionTimeout;
    private final Optional<Duration> socketTimeout;
    private final int numPrefetchedElements;
    private final int numFollowRedirects;
    

    private ClientSseSource(URI uri,
                            Optional<String> lastEventId,
                            boolean isFailOnConnectError,
                            int numFollowRedirects,
                            int numPrefetchedElements,
                            Optional<Duration> connectionTimeout,
                            Optional<Duration> socketTimeout) {
        this.uri = uri;
        this.lastEventId = lastEventId;
        this.isFailOnConnectError = isFailOnConnectError;
        this.numFollowRedirects = numFollowRedirects;
        this.numPrefetchedElements = numPrefetchedElements;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
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
             Optional.empty(),
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
                                   Optional.of(connectionTimeout), 
                                   this.socketTimeout);
    }

    /**
     * @param socketTimeout the socket timeout
     * @return a new instance with the updated behavior
     */
    public ClientSseSource socketTimeout(Duration socketTimeout) {
        return new ClientSseSource(this.uri, 
                                   this.lastEventId, 
                                   this.isFailOnConnectError,
                                   this.numFollowRedirects,
                                   this.numPrefetchedElements,
                                   this.connectionTimeout, 
                                   Optional.of(socketTimeout));
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
                                   this.connectionTimeout, 
                                   this.socketTimeout);
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
                                   this.connectionTimeout, 
                                   this.socketTimeout);
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
                                   this.connectionTimeout, 
                                   this. socketTimeout);
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
                                   this.connectionTimeout, 
                                   this.socketTimeout);
    }

    
    /**
     * @return the new source instance
     * @throws ConnectException if an connect error occurs
     */
    public ReactiveSource<ServerSentEvent> open() throws ConnectException {
        try {
            return openAsync().get();
        } catch (InterruptedException e) {
            throw new ConnectException(e);
        } catch (ExecutionException e) {
            throw new ConnectException(e.getCause());
        }
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
        
        SseInboundStreamSubscription.newSseInboundStreamSubscription(uri, 
                                                                     lastEventId, 
                                                                     isFailOnConnectError,
                                                                     numFollowRedirects,
                                                                     numPrefetchedElements,
                                                                     connectionTimeout, 
                                                                     socketTimeout, 
                                                                     subscriber);
    }

    
    
    
    private static class SseInboundStreamSubscription implements Subscription {
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        private final String id = "cl-in-" + UUID.randomUUID().toString();
     
        // properties
        private final boolean isFailOnConnectError;
        private final int numPrefetchedElements;
        private final Optional<Duration> connectionTimeout;
        private final Optional<Duration> socketTimeout;
        private final URI uri;
        private final int numFollowRedirects;
        private Optional<String> lastEventId;
        
        // incoming event handling
        private final Queue<ServerSentEvent> bufferedEvents = Lists.newLinkedList();
        private final Object consumesLock = new Object();
        private final AtomicInteger numRequested = new AtomicInteger(0);
        private final ServerSentEventParser parser = new ServerSentEventParser();
        private final SubscriberNotifier<ServerSentEvent> subscriberNotifier; 
        
        // underlying stream
        private final RetrySequence retrySequence = new RetrySequence(0, 50, 250, 500, 1000, 2000, 3000);
        private final StreamProvider streamProvider = NettyBasedStreamProvider.newStreamProvider();
        private final AtomicReference<StreamProvider.InboundStream> inboundStreamReference = new AtomicReference<>(new StreamProvider.EmptyInboundStream());
        
        
        
        public static void newSseInboundStreamSubscription(URI uri, 
                                                           Optional<String> lastEventId, 
                                                           boolean isFailOnConnectError,
                                                           int numFollowRedirects,
                                                           int numPrefetchedElements,
                                                           Optional<Duration> connectionTimeout, 
                                                           Optional<Duration> socketTimeout, 
                                                           Subscriber<? super ServerSentEvent> subscriber) {
            
            SseInboundStreamSubscription inboundStreamSubscription = new SseInboundStreamSubscription(uri,
                                                                                                      lastEventId,
                                                                                                      isFailOnConnectError,
                                                                                                      numFollowRedirects,
                                                                                                      numPrefetchedElements,
                                                                                                      connectionTimeout,
                                                                                                      socketTimeout, 
                                                                                                      subscriber);
            inboundStreamSubscription.init();
        }
        
    
        private SseInboundStreamSubscription(URI uri, 
                                             Optional<String> lastEventId,
                                             boolean isFailOnConnectError,
                                             int numFollowRedirects,
                                             int numPrefetchedElements,
                                             Optional<Duration> connectionTimeout, 
                                             Optional<Duration> socketTimeout, 
                                             Subscriber<? super ServerSentEvent> subscriber) {
            this.uri = uri;
            this.lastEventId = lastEventId;
            this.numFollowRedirects = numFollowRedirects;
            this.isFailOnConnectError = isFailOnConnectError;
            this.numPrefetchedElements = numPrefetchedElements;
            this.connectionTimeout = connectionTimeout;
            this.socketTimeout = socketTimeout;
            this.subscriberNotifier = new SubscriberNotifier<>(subscriber, this);
        }
        
        
        public void init() {
            newChannelAsync(Duration.ZERO)
                        .whenComplete((stream, error) -> { 
                                                            if (error == null) {
                                                                setUnderlyingChannel(stream); 
                                                                subscriberNotifier.start();
                                                                
                                                            } else if (isFailOnConnectError) {
                                                                subscriberNotifier.startWithError(error);
                                                                
                                                            } else {
                                                                subscriberNotifier.start();
                                                                resetUnderlyingChannel(Duration.ZERO);
                                                            }
                                                         });
        }
 
        
        @Override
        public void cancel() {
            closeUnderlyingChannel();
            
            if (isOpen.getAndSet(false)) {
                LOG.debug("[" + id + "] close");
                streamProvider.closeAsync();
            }
        } 
       
        
        private void setUnderlyingChannel(InboundStream stream) {
            inboundStreamReference.getAndSet(stream).close();
            refreshFlowControl();
        }        
       
       
        private void closeUnderlyingChannel() {
            if (isConnected()) {
                LOG.debug("[" + id + "] close underlying channel");
            }
            setUnderlyingChannel(new StreamProvider.EmptyInboundStream());
        }
        
        
        private void resetUnderlyingChannel(Duration delay) {
            closeUnderlyingChannel();

            if (isOpen.get()) {
                LOG.debug("[" + id + "] schedule reconnect in " + delay.toMillis() + " millis");
                Runnable retryConnect = () -> newChannelAsync(retrySequence.nextDelay(delay))
                                                    .whenComplete((stream, error) -> { 
                                                                                        if (error == null) {
                                                                                            setUnderlyingChannel(stream);
                                                                                        } else {
                                                                                            resetUnderlyingChannel(retrySequence.nextDelay(delay));
                                                                                        }
                                                                                     });
                
                ScheduledExceutor.common().schedule(retryConnect, delay.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
       
        
        
        private CompletableFuture<StreamProvider.InboundStream> newChannelAsync(Duration retryDelay) {
            if (isOpen.get()) {
                LOG.debug("[" + id + "] open underlying channel with last event id " + lastEventId.orElse(""));
                
                InboundStreamHandler handler = new InboundStreamHandler() {
                  
                    @Override
                    public void onContent(ByteBuffer[] buffers) {
                        processNetworkdata(buffers);
                    }
                    
                    @Override
                    public void onError(Throwable error) {
                        LOG.debug("error occured. reset underlying channel", error);
                        onCompleted();
                    }
                    
                    @Override
                    public void onCompleted() {
                        resetUnderlyingChannel(retryDelay);
                    }
                };
                
                return streamProvider.openInboundStreamAsync(id, 
                                                             uri, 
                                                             lastEventId,
                                                             isFailOnConnectError,
                                                             numFollowRedirects,
                                                             handler,
                                                             connectionTimeout,
                                                             socketTimeout);
            } else {
                return CompletableFuture.completedFuture(new StreamProvider.EmptyInboundStream());
            }
        }
        
        
        
        private void processNetworkdata(ByteBuffer[] buffers) {
            if (isEmpty(buffers)) {
                return;
            }
            
            synchronized (consumesLock) {
                for (int i = 0; i < buffers.length; i++) {
                    ImmutableList<ServerSentEvent> events = parser.parse(buffers[i]);
                    for (ServerSentEvent event : events) {
                        LOG.debug("[" + id + "] event " + event.getId().orElse("") + " received");
                        bufferedEvents.add(event);
                        lastEventId = event.getId(); 
                    }
                }
                
                refreshFlowControl();
            }
            
            process();
        }
        

        private boolean isEmpty(ByteBuffer[] buffers) {
            if (buffers.length > 0) {
                for (int i = 0; i < buffers.length; i++) {
                    if (buffers[i].remaining() > 0) {
                        return false;
                    }
                }
            }
            return true;
        }

        
        private void process() {
            
            synchronized (consumesLock) {

                while (numRequested.get() > 0) {
                    refreshFlowControl();
                    
                    if (bufferedEvents.isEmpty()) {
                        return;
                    } else {
                        ServerSentEvent event = bufferedEvents.poll();
                        numRequested.decrementAndGet();
                        subscriberNotifier.notifyOnNext(event);
                    }
                }
            }
        }
        
        
        @Override
        public void request(long n) {
            if (isOpen.get()) {
                if(n < 0) {
                    // https://github.com/reactive-streams/reactive-streams#3.9
                    subscriberNotifier.notifyOnError((new IllegalArgumentException("Non-negative number of elements must be requested: https://github.com/reactive-streams/reactive-streams#3.9")));
                } else {
                    numRequested.addAndGet((int) n);
                    process();
                }
            } else {
                subscriberNotifier.notifyOnError((new IllegalArgumentException("Stream is closed")));
            }
        }



        
        private void refreshFlowControl() {
            
            synchronized (consumesLock) {
                
                // [Flow-control] will be suspended, if num pre-fetched more than requested ones
                if ((bufferedEvents.size() > getMaxBuffersize()) && !inboundStreamReference.get().isSuspended()) {
                    inboundStreamReference.get().suspend();
                
                 // [Flow-control] will be resumed, if num pre-fetched less than one or 25% of the max buffer size
                } else  if (inboundStreamReference.get().isSuspended() && ( (bufferedEvents.size() < 1) || (bufferedEvents.size() < getMaxBuffersize() * 0.25)) ) {
                    inboundStreamReference.get().resume();
                }
            }
        }
        
        private int getMaxBuffersize() {
            return numRequested.get() + numPrefetchedElements;
        }
        

        private boolean isConnected() {
            return !(inboundStreamReference.get() instanceof StreamProvider.EmptyInboundStream);
        }
        
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            
            if (!isOpen.get()) {
                sb.append("[closed] ");
                
            } else if (isConnected()) {
                sb.append(inboundStreamReference.get().isSuspended() ? "[suspended] " : "");
                
            } else {
                sb.append("[not connected] ");
            }
            
            return sb.append("buffered events: " + bufferedEvents.size() + ", num requested: " + numRequested.get()).toString();
        }
    }
    
    
    
    
    private static final class RetrySequence {
        private ImmutableMap<Duration, Duration> delayMap;
        
        public RetrySequence(int... delaysMillis) {
            Map<Duration, Duration> map = Maps.newHashMap();
            
            for (int i = 0; i < delaysMillis.length; i++) {
                if (delaysMillis.length > (i+1)) {
                    map.put(Duration.ofMillis(delaysMillis[i]), Duration.ofMillis(delaysMillis[i+1]));
                } else {
                    map.put(Duration.ofMillis(delaysMillis[i]), Duration.ofMillis(delaysMillis[i]));
                }
            }
            
            delayMap = ImmutableMap.copyOf(map);
        }
        
        public Duration nextDelay(Duration previous) {
            Duration newDelay = delayMap.get(previous);
            return (newDelay == null) ? previous : newDelay;
        }
    }
}  