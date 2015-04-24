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
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.collect.Lists;

import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.ServerSentEventParser;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;



class SseInboundStreamSubscription implements Subscription {
    private final Queue<ServerSentEvent> bufferedEvents = Lists.newLinkedList();
    private final SubscriberNotifier subscriberNotifier;
    private final InboundStream httpDownstream;
    
    private final Object consumesLock = new Object();
    private final AtomicInteger numRequested = new AtomicInteger(0);
    private final ServerSentEventParser parser = new ServerSentEventParser();

    

    public SseInboundStreamSubscription(URI uri, 
                                        StreamProvider streamProvider, 
                                        Subscriber<? super ServerSentEvent> subscriber) {
        
        this.httpDownstream = new ReconnectingInboundStream(uri,
                                                            streamProvider,
                                                            buffers -> processNetworkdata(buffers));
        subscriberNotifier = new SubscriberNotifier(subscriber);
        emitNotification(new OnSubscribe());
    }
    
    @Override
    public void cancel() {
        httpDownstream.close();
        // TODO httpDownstream should support close listener
    } 
    
    
    @Override
    public void request(long n) {
        if(n <= 0) {
            // https://github.com/reactive-streams/reactive-streams#3.9
            subscriberNotifier.emitNotification(new OnError(new IllegalArgumentException("Non-negative number of elements must be requested: https://github.com/reactive-streams/reactive-streams#3.9")));
        } else {
            numRequested.addAndGet((int) n);
            process();
        }
    }
    
    private void process() {
        
        synchronized (consumesLock) {
            
            
            while (numRequested.get() > 0) {
        
                // [Flow-control] will be resumed, if num prefetched less than one or 25% of the requested ones  
                if (httpDownstream.isSuspended() && ( (bufferedEvents.size() < 1) || (bufferedEvents.size() < numRequested.get() * 0.25)) ) {
                    httpDownstream.resume();
                }
                
                if (bufferedEvents.isEmpty()) {
                    return;
                } else {
                    ServerSentEvent event = bufferedEvents.poll();
                    numRequested.decrementAndGet();
                    emitNotification(new OnNext(event));
                }
            }
        }
    }
    
    
    private void processNetworkdata(ByteBuffer[] buffers) {
        if (isEmpty(buffers)) {
            return;
        }
        
        synchronized (consumesLock) {
            for (int i = 0; i < buffers.length; i++) {
                parser.parse(buffers[i]).forEach(event -> bufferedEvents.add(event));
            }
            
            // [Flow-control] will be suspended, if num prefetched more than requested ones  
            if ((bufferedEvents.size() > numRequested.get()) && !httpDownstream.isSuspended()) {
                httpDownstream.suspend();
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
    
    
    private void emitNotification(SubscriberNotifier.Notification notification) {
        subscriberNotifier.emitNotification(notification);
    }
    
    
    @Override
    public String toString() {
        return (httpDownstream.isSuspended() ? "[suspended] " : "") +  "buffered events: " + bufferedEvents.size() + ", num requested: " + numRequested.get();
    }


    
    private class OnSubscribe extends SubscriberNotifier.Notification {
        
        @Override
        public void signalTo(Subscriber<? super ServerSentEvent> subscriber) {
            subscriber.onSubscribe(SseInboundStreamSubscription.this);
        }
    }
    
    private class OnNext extends SubscriberNotifier.Notification {
        private final ServerSentEvent event;
        
        public OnNext(ServerSentEvent event) {
            this.event = event;
        }
        
        @Override
        public void signalTo(Subscriber<? super ServerSentEvent> subscriber) {
            subscriber.onNext(event);
        }
    }
    
    
    private class OnError extends SubscriberNotifier.TerminatingNotification {
        private final Throwable error;
        
        public OnError(Throwable error) {
            this.error = error;
        }
        
        @Override
        public void signalTo(Subscriber<? super ServerSentEvent> subscriber) {
            subscriber.onError(error);
        }
    }
    

    private class OnComplete extends SubscriberNotifier.TerminatingNotification {
        
        @Override
        public void signalTo(Subscriber<? super ServerSentEvent> subscriber) {
            subscriber.onComplete();
        }
    }
    
    
    private static final class ReconnectingInboundStream implements InboundStream  {
        private final URI uri;
        private final StreamProvider streamProvider;
        private final Consumer<ByteBuffer[]> dataConsumer;
        private InboundStream httpDownstream;

        
        public ReconnectingInboundStream(URI uri, StreamProvider streamProvider, Consumer<ByteBuffer[]> dataConsumer) {
            this.uri = uri;
            this.streamProvider = streamProvider;
            this.dataConsumer = dataConsumer;
            httpDownstream = streamProvider.newInboundStream(uri, dataConsumer, error -> reconnect());
        }
        
        @Override
        public boolean isSuspended() {
            return httpDownstream.isSuspended();
        }
        
        @Override
        public void suspend() {
            httpDownstream.suspend();
        }
        
        @Override
        public void resume() {
            httpDownstream.resume();
        }
        
        @Override
        public void terminate() {
            httpDownstream.terminate();
        }
        
        @Override
        public void close() {
            httpDownstream.close();
        }
        
        private synchronized void reconnect() {
            terminate();
            httpDownstream = streamProvider.newInboundStream(uri, dataConsumer, error -> reconnect());
        }
    }
}
