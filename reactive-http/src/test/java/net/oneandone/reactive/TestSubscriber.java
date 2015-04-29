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




public class TestSubscriber<T> implements Subscriber<T> {
    private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>(); 
    private final List<T> events = Collections.synchronizedList(Lists.newArrayList());
    private final Set<WaitForElementsPromise> waitForElementsPromises = Sets.newCopyOnWriteArraySet();
    
    private boolean isSuspended = false;

 
    
    public void suspend() {
        isSuspended = true;
    }
    
    
    public void resume() {
        isSuspended = false;
        subscriptionRef.get().request(1);
    }
    
    
    public void close() {
        subscriptionRef.get().cancel();
    }
    
    
    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscriptionRef.set(subscription);
        subscription.request(1);
    }
    
    
    @Override
    public void onNext(T event) {
        synchronized (waitForElementsPromises) {
            try {
                System.out.print("TestSubscriber reveived: " + event);
                events.add(event);
                waitForElementsPromises.forEach(promise -> promise.onNext(event));
            } finally {
                if (!isSuspended) {
                    subscriptionRef.get().request(1);
                }
            }
        }
    }
    
    @Override
    public void onError(Throwable t) {
        synchronized (waitForElementsPromises) {
            waitForElementsPromises.forEach(promise -> promise.completeExceptionally(t));
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
        String msg = "num received: " + getNumReceived() + "\r\nsubscription "+ subscriptionRef.get().toString();
        if (isSuspended) {
            msg = "[suspended] " + msg;
        }
        return msg;
    }
  
    
    public CompletableFuture<ImmutableList<T>> getEventsAsync(int numWaitfor) {
        synchronized (waitForElementsPromises) {
            WaitForElementsPromise promise = new WaitForElementsPromise(numWaitfor);
            
            
            events.forEach(event -> promise.onNext(event));
            waitForElementsPromises.add(promise);
            
            return promise;
        }        
    }
    
    
    private final class WaitForElementsPromise extends CompletableFuture<ImmutableList<T>> {
        
        private final AtomicInteger waitFor;
        
        public WaitForElementsPromise(int numWaitfor) {
            this.waitFor = new AtomicInteger(numWaitfor);
        }
        
        void onNext(T element) {
            if (waitFor.decrementAndGet() <= 0)  {
                complete(ImmutableList.copyOf(events)); 
            }
        }
        
    }
   
}
