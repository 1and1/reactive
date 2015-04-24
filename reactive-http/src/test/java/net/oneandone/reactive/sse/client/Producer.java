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



import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.Queues;






public class Producer<T> implements Publisher<T>, Closeable {
    
    private final CompletableFuture<Emitter<T>> promise = new CompletableFuture<>();
    private MySubscription<T> subscription;

    private boolean subscribed = false; // true after first subscribe
    

    public CompletableFuture<Emitter<T>> getEmitterAsync() {
        return promise;
    }
    
    
    public static interface Emitter<T> extends Closeable {
        public void publish(T t);
    }
    
        
    
    @Override
    public void close() throws IOException {
        try {
            promise.get().close();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
    
    
    @Override
    public void subscribe(Subscriber<? super T> subscriber) {       
        synchronized (this) {
            if (subscribed == true) {
                subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
            } else {
                subscribed = true;
                subscriber.onSubscribe(new MySubscription<>((Subscriber<T>) subscriber, promise));
            }
        }
    }
    
    
    
    
    
    
    private static class MySubscription<T> implements Subscription, Emitter<T> {
        private final SubscriberNotifier<T> subscriberNotifier;
        
        public MySubscription(Subscriber<T> subscriber, CompletableFuture<Emitter<T>> promise) {
            subscriberNotifier = new SubscriberNotifier<>(subscriber);
            emitNotification(new OnSubscribe());
            promise.complete(this);
        }

        private void emitNotification(SubscriberNotifier.Notification<T> notification) {
            subscriberNotifier.emitNotification(notification);
        }

        
        @Override
        public void close() throws IOException {
            emitNotification(new OnComplete());
        }
        
        @Override
        public void publish(T obj) {
            subscriberNotifier.emitNotification(new OnNext(obj));
        }
        
        @Override
        public void cancel() {

        }
        
        @Override
        public void request(long n) {

        }
        
        
        
        private class OnSubscribe extends SubscriberNotifier.Notification<T> {
            
            @Override
            public void signalTo(Subscriber<T> subscriber) {
                subscriber.onSubscribe(MySubscription.this);
            }
        }
        
        
        private class OnNext extends SubscriberNotifier.Notification<T> {
            private final T obj;
            
            public OnNext(T obj) {
                this.obj = obj;
            }
            
            @Override
            public void signalTo(Subscriber<T> subscriber) {
                subscriber.onNext(obj);
            }
        }
        
        
        
        private class OnError extends SubscriberNotifier.TerminatingNotification<T> {
            private final Throwable error;
            
            public OnError(Throwable error) {
                this.error = error;
            }
            
            @Override
            public void signalTo(Subscriber<T> subscriber) {
                subscriber.onError(error);
            }
        }
        

        private class OnComplete extends SubscriberNotifier.TerminatingNotification<T> {
            
            @Override
            public void signalTo(Subscriber<T> subscriber) {
                subscriber.onComplete();
            }
        }

        

        private static final class SubscriberNotifier<T> implements Runnable {
            private final ConcurrentLinkedQueue<Notification<T>> notifications = Queues.newConcurrentLinkedQueue();
            private final AtomicBoolean isOpen = new AtomicBoolean(true);
            
            private final Subscriber<T> subscriber;
            
            public SubscriberNotifier(Subscriber<T> subscriber) {
                this.subscriber = subscriber;
            }
            
            private void close() {
                isOpen.set(false);
                notifications.clear();  
            }
            
            public void emitNotification(Notification<T> notification) {
                if (isOpen.get()) {
                    if (notifications.offer(notification)) {
                        tryScheduleToExecute();
                    }
                }
            }

            private final void tryScheduleToExecute() {
                try {
                    ForkJoinPool.commonPool().execute(this);
                } catch (Throwable t) {
                    close(); // no further notifying (executor does not work anyway)
                    subscriber.onError(t);
                }
            }
            

            // main "event loop" 
            @Override 
            public final void run() {
                
                if (isOpen.get()) {
                    
                    synchronized (subscriber) {
                        try {
                            Notification<T> notification = notifications.poll(); 
                            if (notification != null) {
                                if (notification.isTerminating()) {
                                    close();
                                }
                                notification.signalTo(subscriber);
                            }
                        } finally {
                            if(!notifications.isEmpty()) {
                                tryScheduleToExecute(); 
                            }
                        }
                    }
                }
            }
          
            
            
            private static abstract class Notification<T> { 
                
                abstract void signalTo(Subscriber<T> subscriber);
                
                boolean isTerminating() {
                    return false;
                }
            };
            
            
            
            // Once a terminal state has been signaled (onError, onComplete) it is REQUIRED that no further signals occur
            private static abstract class TerminatingNotification<T> extends SubscriberNotifier.Notification<T> { 
                
                boolean isTerminating() {
                    return true;
                }
            };

        }
    }
}        