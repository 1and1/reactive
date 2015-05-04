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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import net.oneandone.reactive.sse.ScheduledExceutor;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.ServerSentEventParser;
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



public class ClientSsePublisher implements Publisher<ServerSentEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ClientSsePublisher.class);
    
    private static final int DEFAULT_NUM_FOILLOW_REDIRECTS = 15;
    
    private final URI uri;
    private boolean isFailOnConnectError;
    private final Optional<String> lastEventId;
    private final Optional<Duration> connectionTimeout;
    private final Optional<Duration> socketTimeout;
    private final int numFollowRedirects;


    public ClientSsePublisher(URI uri) {
        this(uri, 
             Optional.empty(), 
             true, 
             DEFAULT_NUM_FOILLOW_REDIRECTS,
             Optional.empty(),
             Optional.empty());
    }
    
    private ClientSsePublisher(URI uri,
                               Optional<String> lastEventId,
                               boolean isFailOnConnectError,
                               int numFollowRedirects,
                               Optional<Duration> connectionTimeout,
                               Optional<Duration> socketTimeout) {
        this.uri = uri;
        this.lastEventId = lastEventId;
        this.isFailOnConnectError = isFailOnConnectError;
        this.numFollowRedirects = numFollowRedirects;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
    }


    public ClientSsePublisher connectionTimeout(Duration connectionTimeout) {
        return new ClientSsePublisher(this.uri, 
                                      this.lastEventId, 
                                      this.isFailOnConnectError,
                                      this.numFollowRedirects,
                                      Optional.of(connectionTimeout), 
                                      this.socketTimeout);
    }

    public ClientSsePublisher socketTimeout(Duration socketTimeout) {
        return new ClientSsePublisher(this.uri, 
                                      this.lastEventId, 
                                      this.isFailOnConnectError,
                                      this.numFollowRedirects,
                                      this.connectionTimeout, 
                                      Optional.of(socketTimeout));
    }

    public ClientSsePublisher withLastEventId(String lastEventId) {
        return new ClientSsePublisher(this.uri,
                                      Optional.ofNullable(lastEventId),
                                      this.isFailOnConnectError,
                                      this.numFollowRedirects,
                                      this.connectionTimeout, 
                                      this.socketTimeout);
    }

    public ClientSsePublisher failOnConnectError(boolean isFailOnConnectError) {
        return new ClientSsePublisher(this.uri,
                                      this.lastEventId,
                                      isFailOnConnectError,
                                      this.numFollowRedirects,
                                      this.connectionTimeout, 
                                      this.socketTimeout);
    }

    public ClientSsePublisher followRedirects(boolean isFollowRedirects) {
        return new ClientSsePublisher(this.uri,
                                      this.lastEventId,
                                      this.isFailOnConnectError,
                                      isFollowRedirects ? DEFAULT_NUM_FOILLOW_REDIRECTS : 0,
                                      this.connectionTimeout, 
                                      this.socketTimeout);
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
                                                                     connectionTimeout, 
                                                                     socketTimeout, 
                                                                     subscriber);
    }

    
    
    private static class SseInboundStreamSubscription implements Subscription {
        private final RetrySequence retrySequence = new RetrySequence(0, 50, 250, 500, 1000, 2000, 3000);
        
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        
        private final String id = "cl-in-" + UUID.randomUUID().toString();
        private final Queue<ServerSentEvent> bufferedEvents = Lists.newLinkedList();
        private final SubscriberNotifier<ServerSentEvent> subscriberNotifier; 
        
        private final boolean isFailOnConnectError;
        private final Optional<Duration> connectionTimeout;
        private final Optional<Duration> socketTimeout;

        private final Object consumesLock = new Object();
        private final AtomicInteger numRequested = new AtomicInteger(0);
        private final ServerSentEventParser parser = new ServerSentEventParser();
    
        private final StreamProvider streamProvider = NettyBasedStreamProvider.newStreamProvider();
        private final URI uri;
        private final int numFollowRedirects;
        private final AtomicReference<StreamProvider.InboundStream> inboundStreamReference = new AtomicReference<>(new StreamProvider.EmptyInboundStream());
        private Optional<String> lastEventId;
        
        
        
        public static void newSseInboundStreamSubscription(URI uri, 
                                                           Optional<String> lastEventId, 
                                                           boolean isFailOnConnectError,
                                                           int numFollowRedirects,
                                                           Optional<Duration> connectionTimeout, 
                                                           Optional<Duration> socketTimeout, 
                                                           Subscriber<? super ServerSentEvent> subscriber) {
            SseInboundStreamSubscription inboundStreamSubscription = new SseInboundStreamSubscription(uri,
                                                                                                      lastEventId,
                                                                                                      isFailOnConnectError,
                                                                                                      numFollowRedirects,
                                                                                                      connectionTimeout,
                                                                                                      socketTimeout, 
                                                                                                      subscriber);
            inboundStreamSubscription.init();
        }
        
    
        private SseInboundStreamSubscription(URI uri, 
                                             Optional<String> lastEventId,
                                             boolean isFailOnConnectError,
                                             int numFollowRedirects,
                                             Optional<Duration> connectionTimeout, 
                                             Optional<Duration> socketTimeout, 
                                             Subscriber<? super ServerSentEvent> subscriber) {
            this.uri = uri;
            this.lastEventId = lastEventId;
            this.numFollowRedirects = numFollowRedirects;
            this.isFailOnConnectError = isFailOnConnectError;
            this.connectionTimeout = connectionTimeout;
            this.socketTimeout = socketTimeout;
            subscriberNotifier = new SubscriberNotifier<>(subscriber, this);
        }
        
        
        public void init() {
            newChannelAsync(Duration.ofMillis(0))
                        .whenComplete((stream, error) -> { 
                                                            if (error == null) {
                                                                inboundStreamReference.getAndSet(stream); 
                                                                subscriberNotifier.start();
                                                            } else if (isFailOnConnectError) {
                                                                subscriberNotifier.startWithError(error);
                                                            } else {
                                                                subscriberNotifier.start();
                                                                new CloseHandler<>(Duration.ofMillis(0)).accept(null);
                                                            }
                                                         });
        }
 
        
        @Override
        public void cancel() {
            closeUnderlyingConnection();
            
            if (isOpen.getAndSet(false)) {
                LOG.debug("[" + id + "] closing");
                streamProvider.closeAsync();
            }
        } 
       
        
        private CompletableFuture<StreamProvider.InboundStream> newChannelAsync(Duration retryDelay) {
            if (isOpen.get()) {
                LOG.debug("[" + id + "] open underyling channel with last event id " + lastEventId.orElse(""));
                
                InboundStreamHandler handler = new InboundStreamHandler() {
                  
                    @Override
                    public void onContent(ByteBuffer[] buffers) {
                        processNetworkdata(buffers);
                    }
                    
                    @Override
                    public void onError(Throwable error) {
                        new CloseHandler<Throwable>(retryDelay).accept(error);
                    }
                    
                    @Override
                    public void onCompleted() {
                        new CloseHandler<Void>(retryDelay).accept(null);
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
        
        
        private final class CloseHandler<T> implements Consumer<T> {
            private final AtomicBoolean isOpen = new AtomicBoolean(true);
            private final Duration retryDelay;
            
            public CloseHandler(Duration retryDelay) {
                this.retryDelay = retryDelay;
            }
            
            @Override
            public void accept(T t) {
                if (isOpen.getAndSet(false)) {
                    resetUnderlyingConnection(retryDelay);
                }
            }
        }
       

        private void closeUnderlyingConnection() {
            if (isConnected()) {
                LOG.debug("[" + id + "] close underlying channel");
            }
            inboundStreamReference.getAndSet(new StreamProvider.EmptyInboundStream()).close();
        }
        
        private void resetUnderlyingConnection(Duration delay) {
            closeUnderlyingConnection();

            if (isOpen.get()) {
                LOG.debug("[" + id + "] schedule reconnect in " + delay.toMillis() + " millis");
                
                
                Runnable retryConnect = () -> newChannelAsync(retrySequence.nextDelay(delay)).whenComplete((stream, error) -> { 
                                                                                                                                if (error == null) {
                                                                                                                                    inboundStreamReference.getAndSet(stream).close();
                                                                                                                                } else {
                                                                                                                                    resetUnderlyingConnection(retrySequence.nextDelay(delay));
                                                                                                                                }
                                                                                                                              });
                
                ScheduledExceutor.common().schedule(retryConnect, delay.toMillis(), TimeUnit.MILLISECONDS);
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
                
                // [Flow-control] will be suspended, if num pre-fetched more than requested ones  
                if ((bufferedEvents.size() > numRequested.get()) && !inboundStreamReference.get().isSuspended()) {
                    inboundStreamReference.get().suspend();
                } 
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
        
  
        
        
        @Override
        public void request(long n) {
            if (isOpen.get()) {
                if(n <= 0) {
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
        
        private void process() {
            
            synchronized (consumesLock) {

                while (numRequested.get() > 0) {
            
                    // [Flow-control] will be resumed, if num prefetched less than one or 25% of the requested ones  
                    if (inboundStreamReference.get().isSuspended() && ( (bufferedEvents.size() < 1) || (bufferedEvents.size() < numRequested.get() * 0.25)) ) {
                        inboundStreamReference.get().resume();
                    }
                    
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