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


import java.nio.channels.ClosedChannelException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import net.oneandone.reactive.utils.SubscriberNotifier;
import net.oneandone.reactive.utils.Utils;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;






class ReactiveSinkSubscription<T> implements Subscription, ReactiveSink<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ReactiveSinkSubscription.class);
    
    private final SubscriberNotifier<T> subscriberNotifier;

    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    
    private final Object processingLock = new Object();
    private final AtomicLong numRequested = new AtomicLong();
    private final Queue<Write> pendingWrites = Lists.newLinkedList();
    
    private final int maxBuffered;
    
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final CompletableFuture<ReactiveSink<T>> startPromise = new CompletableFuture<>(); 

    
    
    static <T> CompletableFuture<ReactiveSink<T>> newSubscriptionAsync(Subscriber<T> subscriber, int maxBuffered) {
        return new ReactiveSinkSubscription<>(subscriber, maxBuffered).init();
    }

    
    private ReactiveSinkSubscription(Subscriber<T> subscriber, int maxBuffered) {
        // https://github.com/reactive-streams/reactive-streams-jvm#1.9
        if (subscriber == null) {  
            throw new NullPointerException("subscriber is null");
        }

        this.maxBuffered = maxBuffered;
        this.subscriberNotifier = new SubscriberNotifier<>(subscriber, this);
    }
    
    
    private CompletableFuture<ReactiveSink<T>> init() {
        this.subscriberNotifier.start();
        return startPromise;
    }

    
    @Override
    public void cancel() {
        if (isOpen.getAndSet(false)) {
            if (!isStarted.getAndSet(true)) {
                startPromise.completeExceptionally(new ClosedChannelException());
            }
            
            subscriberNotifier.notifyOnComplete();
        }
    }

    @Override
    public void request(long n) {
        if (isOpen.get()) {
            synchronized (processingLock) {
                if (!isStarted.getAndSet(true)) {
                    startPromise.complete(this);
                }
                
                numRequested.addAndGet(n);                
                process();
            }
        } else {
            LOG.debug("request(num) is called with " +  n + " even though sink is closed. Ignore request ");
            cancel();
        }
    }

    
    private void process() {
        synchronized (processingLock) {
            while (numRequested.get() > 0) {
                Write write = pendingWrites.poll();
                if (write == null) {
                    return;
                } else {
                    write.perform();
                }
            }
        }
    }



    @Override
    public boolean isWriteable() {
        synchronized (processingLock) {
            return isOpen() && (getCapacity() > 0);
        }
    }
    
    
    private long getCapacity() {
        int remainingBufferSpace = maxBuffered - pendingWrites.size();
        return remainingBufferSpace + numRequested.get();
    }
 
    
    
    @Override
    public CompletableFuture<Void> writeAsync(T element) {
        if (isOpen.get()) {
            Write write = new Write(element);

            synchronized (processingLock) {
                if (getCapacity() <= 0) {
                    throw new IllegalStateException("capacitiy limit reached (maxBuffered: " + maxBuffered + ", numRequested: " + numRequested.get() + " pending: " + pendingWrites.size() + ")");
                }

                pendingWrites.offer(write);
                process();
            }
            
            return write;

        } else {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            promise.completeExceptionally(new IllegalStateException("sink is closed"));
            return promise;
        }
    }
    
    
    @Override
    public void write(T element) {
        try {
            writeAsync(element).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw Utils.propagate(e);
        }
    }

    
    @Override
    public boolean isOpen() {
        return isOpen.get();
    }
    
    @Override
    public void close() {
        shutdown();
    }
    
    public void shutdown() {
        cancel();
    }
    
    @Override
    public ImmutableList<T> shutdownNow() {
        ImmutableList<T> pending = getPendingWrites();
        cancel();
        
        return pending;
    }

    
    private ImmutableList<T> getPendingWrites() {
        synchronized (processingLock) {
            return ImmutableList.copyOf(pendingWrites.stream().map(write -> write.getElement()).collect(Collectors.toList()));
        }
    }
    
   
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (!isOpen.get()) {
            builder.append("[closed] " );
        }
        builder.append("running writes=" + pendingWrites.size() + " numRequested=" + numRequested + ", maxBuffered: " + maxBuffered + ", capacitiy: " + getCapacity());
        builder.append("  (subscription: " + subscriberNotifier.toString() + ")");
            
        return builder.toString();
    }
    
    
    private final class Write extends CompletableFuture<Void> {
 
        private final T element;
        
        public Write(T element) {
            this.element = element;
        }
        
        public T getElement() {
            return element;
        }
        
        
        public void perform() {
            try {
                subscriberNotifier.notifyOnNext(element);
                numRequested.decrementAndGet();
                complete(null);
            } catch (RuntimeException rt) {
                completeExceptionally(rt);
            }
        }
    }
}
