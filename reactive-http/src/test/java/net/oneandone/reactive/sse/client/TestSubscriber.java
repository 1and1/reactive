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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import net.oneandone.reactive.sse.ServerSentEvent;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;




class TestSubscriber implements Subscriber<ServerSentEvent> {
    private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>(); 
    private final List<ServerSentEvent> events = Collections.synchronizedList(Lists.newArrayList());
    
    private final CompletableFuture<ImmutableList<ServerSentEvent>> promise = new CompletableFuture<>();
    private final int numPrefetch;
    private final int numWaitFor;
    private boolean isSuspended = false;
    
    public TestSubscriber(int numPrefetch, int numWaitFor) {
        this.numPrefetch = numPrefetch;
        this.numWaitFor = numWaitFor;
    }
    
    
    void suspend() {
        isSuspended = true;
    }
    
    
    void resume() {
        isSuspended = false;
        subscriptionRef.get().request(1);
    }
    
    
    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscriptionRef.set(subscription);
        subscription.request(numPrefetch);
    }
    
    @Override
    public void onNext(ServerSentEvent event) {
        try {
            events.add(event);
            if (events.size() == numWaitFor) {
                promise.complete(ImmutableList.copyOf(events));
            }
        } finally {
            if (!isSuspended) {
                subscriptionRef.get().request(1);
            }
        }
    }
    
    @Override
    public void onError(Throwable t) {
        promise.completeExceptionally(t);
    }
    
    @Override
    public void onComplete() {

    }
    
    public Subscription getSubscription() {
        return subscriptionRef.get();
    }
    
    public CompletableFuture<ImmutableList<ServerSentEvent>> getEventsAsync() {
        return promise;
    }
    
    public int getNumReceived() {
        return events.size();
    }
    
    @Override
    public String toString() {
        return "num received: " + getNumReceived() + "\r\nsubscription "+ subscriptionRef.get().toString();
    }
}
