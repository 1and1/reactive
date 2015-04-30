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
    
    private boolean subscribed = false; // true after first subscribe
    
    private final URI uri;
    private final Optional<String> lastEventId;
    private final Optional<Duration> connectionTimeout;
    private final Optional<Duration> socketTimeout;


    public ClientSsePublisher(URI uri) {
        this(uri, Optional.empty(), Optional.empty(), Optional.empty());
    }
    
    private ClientSsePublisher(URI uri,
                               Optional<String> lastEventId,
                               Optional<Duration> connectionTimeout,
                               Optional<Duration> socketTimeout) {
        this.uri = uri;
        this.lastEventId = lastEventId;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
    }


    public ClientSsePublisher connectionTimeout(Duration connectionTimeout) {
        return new ClientSsePublisher(this.uri, this.lastEventId, Optional.of(connectionTimeout), this.socketTimeout);
    }

    public ClientSsePublisher socketTimeout(Duration socketTimeout) {
        return new ClientSsePublisher(this.uri, this.lastEventId, this.connectionTimeout, Optional.of(socketTimeout));
    }



    public ClientSsePublisher withLastEventId(String lastEventId) {
        return new ClientSsePublisher(this.uri,
                                      Optional.ofNullable(lastEventId), 
                                      this.connectionTimeout, 
                                      this.socketTimeout);
    }


    
    
    
    @Override
    public void subscribe(Subscriber<? super ServerSentEvent> subscriber) {
        synchronized (this) {
            // https://github.com/reactive-streams/reactive-streams-jvm#1.9
            if (subscriber == null) {  
                throw new NullPointerException("subscriber is null");
            }
            
            if (subscribed == true) {
                subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
            } else {
                subscribed = true;
                SseInboundStreamSubscription.newSseInboundStreamSubscription(uri, lastEventId, connectionTimeout, socketTimeout, subscriber);
            }
        }   
    }

    
    private static class SseInboundStreamSubscription implements Subscription {
        private final RetrySequence retrySequence = new RetrySequence(0, 100, 300, 500, 1000, 2000, 3000);
        
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        
        private final String id = "cl-in-" + UUID.randomUUID().toString();
        private final Queue<ServerSentEvent> bufferedEvents = Lists.newLinkedList();
        private final SubscriberNotifier<ServerSentEvent> subscriberNotifier; 
        
        private final Optional<Duration> connectionTimeout;
        private final Optional<Duration> socketTimeout;

        private final Object consumesLock = new Object();
        private final AtomicInteger numRequested = new AtomicInteger(0);
        private final ServerSentEventParser parser = new ServerSentEventParser();
    
        private final StreamProvider streamProvider = NettyBasedStreamProvider.newStreamProvider();
        private final URI uri;
        private final AtomicReference<StreamProvider.InboundStream> inboundStreamReference = new AtomicReference<>(new StreamProvider.EmptyInboundStream());
        private Optional<String> lastEventId;
        
        
        
        public static void newSseInboundStreamSubscription(URI uri, Optional<String> lastEventId, Optional<Duration> connectionTimeout, Optional<Duration> socketTimeout, Subscriber<? super ServerSentEvent> subscriber) {
            SseInboundStreamSubscription inboundStreamSubscription = new SseInboundStreamSubscription(uri, lastEventId, connectionTimeout, socketTimeout, subscriber);
            inboundStreamSubscription.init();
        }
        
    
        private SseInboundStreamSubscription(URI uri, Optional<String> lastEventId, Optional<Duration> connectionTimeout, Optional<Duration> socketTimeout, Subscriber<? super ServerSentEvent> subscriber) {
            this.uri = uri;
            this.lastEventId = lastEventId;
            this.connectionTimeout = connectionTimeout;
            this.socketTimeout = socketTimeout;
            subscriberNotifier = new SubscriberNotifier<>(subscriber, this);
        }
        
        public void init() {
            newChannelAsync(Duration.ofMillis(0))
                        .thenAccept(stream -> { 
                                                inboundStreamReference.getAndSet(stream); 
                                                subscriberNotifier.start();  // will start/notify subscriber only, if inbound connection is established  
                                              });
        }
 
     
        
        @Override
        public void cancel() {
            inboundStreamReference.getAndSet(new StreamProvider.EmptyInboundStream()).close();
            
            if (isOpen.getAndSet(false)) {
                LOG.debug("[" + id + "] closing");
                streamProvider.close();
            }
        } 
       
        
        private CompletableFuture<StreamProvider.InboundStream> newChannelAsync(Duration retryDelay) {
            if (isOpen.get()) {
                LOG.debug("[" + id + "] open underyling channel with last event id " + lastEventId.orElse(""));
                return streamProvider.openInboundStreamAsync(id, 
                                                             uri, 
                                                             lastEventId, 
                                                             buffers -> processNetworkdata(buffers), 
                                                             new CloseHandler<Void>(retryDelay),
                                                             new CloseHandler<Throwable>(retryDelay),
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
       
        
        private void resetUnderlyingConnection(Duration delay) {
            LOG.debug("[" + id + "] close underyling channel");
            inboundStreamReference.getAndSet(new StreamProvider.EmptyInboundStream()).close();

            if (isOpen.get()) {
                LOG.debug("[" + id + "] schedule reconnect in " + delay.toMillis() + " millis");
                
                ScheduledExceutor.common().schedule(() -> newChannelAsync(retrySequence.nextDelay(delay)).thenAccept(stream -> inboundStreamReference.getAndSet(stream).close()), 
                                                    delay.toMillis(), 
                                                    TimeUnit.MILLISECONDS);
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
        
        
      
        
        @Override
        public String toString() {
            return (inboundStreamReference.get().isSuspended() ? "[suspended] " : "") +  "buffered events: " + bufferedEvents.size() + ", num requested: " + numRequested.get();
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