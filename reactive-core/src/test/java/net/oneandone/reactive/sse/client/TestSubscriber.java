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



import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;




class TestSubscriber<T> implements Subscriber<T> {
    private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>(); 
    private final List<T> events = Collections.synchronizedList(Lists.newArrayList());
    private final Set<Promise> promises = Sets.newCopyOnWriteArraySet();
    
    private final int numPrefetch;
    private boolean isSuspended = false;

        
    
    public TestSubscriber() {
        this(Integer.MAX_VALUE);
    }
        
    public TestSubscriber(int numPrefetch) {
        this.numPrefetch = numPrefetch;
    }
    
    
    void suspend() {
        isSuspended = true;
    }
    
    
    void resume() {
        isSuspended = false;
        subscriptionRef.get().request(1);
    }
    
    
    public void close() {
        subscriptionRef.get().cancel();
    }
    
    
    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscriptionRef.set(subscription);
        subscription.request(numPrefetch);
    }
    
    @Override
    public void onNext(T event) {
        synchronized (events) {
            try {
                System.out.print("TestSubscriber reveived: " + event);
                events.add(event);
                promises.forEach(promise -> promise.onNext(event));
            } finally {
                if (!isSuspended) {
                    subscriptionRef.get().request(1);
                }
            }
        }
    }
    
    @Override
    public void onError(Throwable t) {
        synchronized (events) {
            promises.forEach(promise -> promise.completeExceptionally(t));
        }
    }
    
    @Override
    public void onComplete() {

    }
    
    public Subscription getSubscription() {
        return subscriptionRef.get();
    }
    
  
    public int getNumReceived() {
        return events.size();
    }
    
    @Override
    public String toString() {
        return "num received: " + getNumReceived() + "\r\nsubscription "+ subscriptionRef.get().toString();
    }
  
    
    public CompletableFuture<ImmutableList<T>> getEventsAsync(int numWaitfor) {
        synchronized (events) {
            Promise promise = new Promise(numWaitfor);
            events.forEach(event -> promise.onNext(event));
            promises.add(promise);
            
            return promise;
        }        
    }
    
    
    private final class Promise extends CompletableFuture<ImmutableList<T>> {
        
        private final AtomicInteger waitFor;
        
        public Promise(int numWaitfor) {
            this.waitFor = new AtomicInteger(numWaitfor);
        }
        
        void onNext(T element) {
            if (waitFor.decrementAndGet() <= 0)  {
                complete(ImmutableList.copyOf(events)); 
            }
        }
        
    }
   
}
