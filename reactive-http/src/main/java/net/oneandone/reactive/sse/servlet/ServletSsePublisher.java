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



import java.util.concurrent.ForkJoinPool;

import javax.servlet.ServletInputStream;

import net.oneandone.reactive.sse.ServerSentEvent;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;



/**
 * Maps the servlet input stream (which have to receive Server-sent events) into a publisher 
 */
public class ServletSsePublisher implements Publisher<ServerSentEvent> {     
    private final ServletInputStream in;
    private boolean subscribed = false; // true after first subscribe
    
    
    /**
     * @param in the underlying servlet input stream
     */
    public ServletSsePublisher(ServletInputStream in) {
        this.in = in;
    }
    
    
    @Override
    public void subscribe(Subscriber<? super ServerSentEvent> subscriber) {
        synchronized (this) {
            if (subscribed == true) {
                subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
            } else {
                subscribed = true;
                subscriber.onSubscribe(new SEEEventReaderSubscription(in, subscriber));
            }
        }
    }
    
  
 
    
    private static final class SEEEventReaderSubscription implements Subscription {
        private final Subscriber<? super ServerSentEvent> subscriberProxy;
        private final SseReadableChannel channel;
        
       
        public SEEEventReaderSubscription(ServletInputStream in, Subscriber<? super ServerSentEvent> subscriber) {
            this.subscriberProxy = new SubscriberProxy(subscriber);
            this.channel = new SseReadableChannel(in,                                        // servlet input stream
                                                  event -> subscriberProxy.onNext(event),    // event consumer
                                                  error -> subscriberProxy.onError(error),   // error consumer
                                                  Void -> subscriberProxy.onComplete());     // completion consumer         
        }
        
        @Override
        public void cancel() {
            subscriberProxy.onComplete();
        }
        
        @Override
        public void request(long n) {
            // Subscription: MUST NOT allow unbounded recursion such as Subscriber.onNext -> Subscription.request -> Subscriber.onNext
            ForkJoinPool.commonPool().execute(() -> requestNext(n));
        }
        
        private void requestNext(long num) {
            for (int i = 0; i < num; i++) {
                try {
                    channel.consumeNextEvent();
                } catch (RuntimeException rt) {
                    subscriberProxy.onError(rt);
                }
            }
        }
      
        
        private final class SubscriberProxy implements Subscriber<ServerSentEvent> {
            private boolean isOpen = true;
            private final Subscriber<? super ServerSentEvent> subscriber;
            
            public SubscriberProxy(Subscriber<? super ServerSentEvent> subscriber) {
                this.subscriber = subscriber;
            }
            
            @Override
            public void onSubscribe(Subscription s) {
            }
            
            @Override
            public synchronized void onNext(ServerSentEvent event) {
                try {
                    if (isOpen) {
                        subscriber.onNext(event);
                    }
                } catch (RuntimeException rt) {
                    onError(rt);
                }   
            }            
            
            
            ////////////
            // Once a terminal state has been signaled (onError, onComplete) it is REQUIRED that no further signals occur
            
            @Override
            public synchronized void onError(Throwable t) {
                if (isOpen) {
                    isOpen = false;
                    subscriber.onError(t);
                    
                    try {
                        channel.close();
                    } catch (RuntimeException ignore) { }
                }
            }

            @Override
            public synchronized void onComplete() {
                if (isOpen) {
                    isOpen = false;
                    subscriber.onComplete();
                    
                    try {
                        channel.close();
                    } catch (RuntimeException ignore) { }
                }            
            }
        }
    }
}
