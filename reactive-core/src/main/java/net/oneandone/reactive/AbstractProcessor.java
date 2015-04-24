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
package net.oneandone.reactive;



import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import net.oneandone.reactive.utils.IllegalStateSubscription;
import net.oneandone.reactive.utils.SubscriberNotifier;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.Queues;



public class AbstractProcessor<T> implements Processor<T, T> {
    private final int buffersize;
    
    private final Object processLock = new Object();
    private final ConcurrentLinkedQueue<T> elements = Queues.newConcurrentLinkedQueue();
    private final AtomicInteger numPendingConsumeRequests = new AtomicInteger(); 

    private final AtomicReference<ConsumingSubscription> consumingSubscriptionRef = new AtomicReference<>();
    
    private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>(new IllegalStateSubscription());

    private boolean subscribed = false; // true after first subscribe
    
    
    
    public AbstractProcessor(int buffersize) {
        this.buffersize = buffersize;
    }

    
    
    /////////////////////////////////////
    // Subscriber
  
    
    @Override
    public void onSubscribe(Subscription subscription) {
        subscriptionRef.set(subscription);
        subscriptionRef.get().request(buffersize); 
    }
    
    
    @Override
    public void onComplete() {
        
    }
    
    @Override
    public void onError(Throwable t) {
        
    }
    
    @Override
    public void onNext(T t) {
        elements.add(t);
        process();
    }
    
    
    
    private void process() {
        
        synchronized (processLock) {
            while (numPendingConsumeRequests.get() > 0) {
                
                if (elements.isEmpty()) {
                    
                } else {
                    T element = elements.poll();
                    try {
                        consumingSubscriptionRef.get().onNext(element);
                    } finally {
                        numPendingConsumeRequests.decrementAndGet();
                    }
                }
            }
        }
    }
    
    
    
    
    /////////////////////////////////////
    // PUBLISHER
    
    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        synchronized (this) {
            // https://github.com/reactive-streams/reactive-streams-jvm#1.9
            if (subscriber == null) {  
                throw new NullPointerException("subscriber is null");
            }
            
            if (subscribed == true) {
                subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
            } else {
                subscribed = true;
                ConsumingSubscription bufferingSubscription = new ConsumingSubscription(subscriber);
                consumingSubscriptionRef.set(bufferingSubscription);
                
                bufferingSubscription.init();
            }
        }
    }
    
    

    private class ConsumingSubscription implements Subscription {
        private final SubscriberNotifier<T> subscriberNotifier;
        
        public ConsumingSubscription(Subscriber<? super T> subscriber) {
            this.subscriberNotifier = new SubscriberNotifier<>(subscriber, this);
        }
        
        public void init() {
            subscriberNotifier.start();
        }

        @Override
        public void cancel() {
        }
        
        
        @Override
        public void request(long n) {
            if(n <= 0) {
                // https://github.com/reactive-streams/reactive-streams#3.9
                subscriberNotifier.notifyOnError(new IllegalArgumentException("Non-negative number of elements must be requested: https://github.com/reactive-streams/reactive-streams#3.9"));
            } else {
                numPendingConsumeRequests.incrementAndGet();
                process();
            }
        }
        
        
        private void onNext(T element) {
            subscriberNotifier.notifyOnNext(element);
        }
    }
}
