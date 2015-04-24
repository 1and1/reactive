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


import javax.servlet.ServletInputStream;

import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.utils.SubscriberNotifier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;



/**
 * Maps the servlet input stream (which have to receive Server-sent events) into a publisher 
 */
class ServletSsePublisher implements Publisher<ServerSentEvent> {
    
    private boolean subscribed = false; // true after first subscribe
    private final ServletInputStream inputStream;

    /**
     * @param inputStream  the underlying servlet input stream
     */
    public ServletSsePublisher(ServletInputStream inputStream) {
        this.inputStream = inputStream;
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
                SEEEventReaderSubscription subscription = new SEEEventReaderSubscription(inputStream, subscriber);
                subscription.init();
            }
        }
    }

    
    
    private static class SEEEventReaderSubscription implements Subscription {
        private final SubscriberNotifier<ServerSentEvent> subscriberNotifier;
        private final SseReadableChannel channel;


        public SEEEventReaderSubscription(ServletInputStream inputStream, Subscriber<? super ServerSentEvent> subscriber) {
            this.subscriberNotifier = new SubscriberNotifier<>(subscriber, this);
            
            this.channel = new SseReadableChannel(inputStream,                                      // servlet input stream
                                                  event -> subscriberNotifier.notifyOnNext(event),    // event consumer
                                                  error -> subscriberNotifier.notifyOnError(error),   // error consumer
                                                  Void -> subscriberNotifier.notifyOnComplete());     // completion consumer         
        }
        
        void init() {
            subscriberNotifier.start();
        }

        @Override
        public void cancel() {
            channel.close();
        }
        
        
        @Override
        public void request(long n) {
            if(n <= 0) {
                // https://github.com/reactive-streams/reactive-streams#3.9
                subscriberNotifier.notifyOnError(new IllegalArgumentException("Non-negative number of elements must be requested: https://github.com/reactive-streams/reactive-streams#3.9"));
            } else {
                channel.request(n);
            }
        }
    }
}        
    

