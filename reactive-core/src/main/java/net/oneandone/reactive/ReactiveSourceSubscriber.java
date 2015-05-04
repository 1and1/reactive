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


import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;





import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.Lists;


class ReactiveSourceSubscriber<T> implements Subscriber<T> {
    
    private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
    private final AtomicReference<EventConsumer<T>> eventConsumerRef = new AtomicReference<>(new InitialEventConsumer());
    
    private final CompletableFuture<ReactiveSource<T>> promise;
    
    public ReactiveSourceSubscriber(CompletableFuture<ReactiveSource<T>> promise) {
        this.promise = promise;
    }
    
    @Override
    public void onSubscribe(Subscription subscription) {
        subscriptionRef.set(subscription);
        
        ReactiveSourceImpl<T> reactiveSource = new ReactiveSourceImpl<>(this);
        promise.complete(reactiveSource);
                
        eventConsumerRef.set(reactiveSource);
    }
    
    @Override
    public void onNext(T event) {
        eventConsumerRef.get().onNext(event);
    }
    
    @Override
    public void onError(Throwable error) {
        eventConsumerRef.get().onError(error);
    }
    
    @Override
    public void onComplete() {
        eventConsumerRef.get().onComplete();
    }

    public void cancel() {
        subscriptionRef.get().cancel();
    }

    public void request(long num) {
        subscriptionRef.get().request(num);
    }

    
    private class InitialEventConsumer implements EventConsumer<T> {
        
        @Override
        public void onError(Throwable error) {
            promise.completeExceptionally(error);
        }
        
        @Override
        public void onNext(T element) {
        }
        
        @Override
        public void onComplete() {
        }           
    }
    
    
    private static interface EventConsumer<T> {
        
        void onNext(T element);
        
        void onError(Throwable error);
        
        void onComplete();
    }
    
    
    
    private static class ReactiveSourceImpl<T> implements ReactiveSource<T>, EventConsumer<T> {
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        private final ReactiveSourceSubscriber<T> source;
        
        private final Object processingLock = new Object();
        private final List<CompletableFuture<T>> pendingReads = Lists.newArrayList();
        private final List<T> inBuffer = Lists.newArrayList();

        private final AtomicReference<Throwable> errorRef = new AtomicReference<>();

        // statistics
        private final AtomicLong numReceived = new AtomicLong(); 

        
        private ReactiveSourceImpl(ReactiveSourceSubscriber<T> source) {
            this.source = source;
        }

        @Override
        public void close() {
            if (isOpen.getAndSet(false)) {
                source.cancel();
            }
        }
        
        
        @Override
        public T read() {
            try {
                return readAsync().get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }
        }

        
        @Override
        public CompletableFuture<T> readAsync() {
            CompletableFuture<T> promise = new CompletableFuture<>(); 

            if (isOpen.get()) {
                
                synchronized (processingLock) {
                    source.request(1);
                    
                    if (errorRef.get() == null) {
                        pendingReads.add(promise);
                        process();
                        
                    } else {
                        promise.completeExceptionally(errorRef.get());
                    }
                }
            } else {
                promise.completeExceptionally(new IllegalStateException("source is closed"));
            }
            
            return promise;
        }
     
        
        
        @Override
        public void onNext(T element) {
            synchronized (processingLock) {
                numReceived.incrementAndGet();
                inBuffer.add(element);
                process();
            }
        }
        
        @Override
        public void onComplete() {
            isOpen.set(false);
        }
        
        @Override
        public void onError(Throwable error) {
            synchronized (processingLock) {
                errorRef.set(error);
                for (CompletableFuture<T> promise : pendingReads) {
                    promise.completeExceptionally(error);
                }
                pendingReads.clear();
            }
        }
        
        private void process() {
            while (!inBuffer.isEmpty() && !pendingReads.isEmpty()) {
                CompletableFuture<T> promise = pendingReads.remove(0);
                promise.complete(inBuffer.remove(0));
            }
        }
        
        
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            if (isOpen.get()) {
                builder.append("[closed] " );
            } else {
                
            }
            
            builder.append(" numReceived=" + numReceived);
            
            return builder.toString();
        }
    }
}
