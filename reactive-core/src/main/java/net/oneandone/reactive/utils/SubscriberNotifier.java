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
package net.oneandone.reactive.utils;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Queues;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;



/**
 * Helper class to call the callback methods of an subscriber
 * 
 * @author grro
 *
 * @param <T> the element type 
 */
public class SubscriberNotifier<T> {
    private final ConcurrentLinkedQueue<Notification<T>> notifications = Queues.newConcurrentLinkedQueue();
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final Scheduler scheduler = new Scheduler(); 
    
    private final Subscriber<? super T> subscriber;
    
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);


    /**
     * Constructor
     * @param subscriber         the subscriber to call
     * @param subscription       the subscription 
     */
    public SubscriberNotifier(Subscriber<? super T> subscriber, Subscription subscription) {
        this.subscriber = subscriber;
        notify(new OnSubscribe<>(subscription));
    }
    
    
    /**
     * start notifying the subscriber
     */
    public void start() {
        isInitialized.set(true);
        scheduler.tryScheduleToExecute();
    }
    
    /**
     * start notifying the subscriber with error
     * @param error the error
     */
    public void start(Throwable error) {
        if (error != null) {
            notifications.clear();
            notifyOnError(error);
        }
        start();
    }

    
    /**
     * perform the onNext callback
     * @param t the element
     */
    public void notifyOnNext(T t) {
        if (t == null) {
            throw new NullPointerException("onNext element to notify is null");
        }
        notify(new OnNext<>(t));
    }

    
    /**
     * perform the onError callback
     * @param error the error to notify
     */
    public void notifyOnError(Throwable error) {
        if (error == null) {
            throw new NullPointerException("onError element to notify is null");
        }
        notify(new OnError<>(error));
    }
    
    
    /**
     * perform the onComplete callback
     */
    public void notifyOnComplete() {
        notify(new OnComplete<>());
    }
    
    
    private void notify(Notification<T> notification) {
        if (isOpen.get()) {
            if (notifications.offer(notification)) {
                scheduler.tryScheduleToExecute();
            }
        }
    }

    
    private void close() {
        isOpen.set(false);
        notifications.clear();  
    }
    
    
    @Override
    public String toString() {
        return subscriber.toString();
    }

    
    private final class Scheduler implements Runnable {
        
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
                            notification.sendTo(subscriber);
                        }
                    } finally {
                        if(!notifications.isEmpty()) {
                            tryScheduleToExecute(); 
                        }
                    }
                }
            }
        }
        
        
        public final void tryScheduleToExecute() {
            if (!isInitialized.get()) {
                return; 
            }
            
            try {
                ForkJoinPool.commonPool().execute(this);
            } catch (Throwable t) {
                close(); // no further notifying (executor does not work anyway)
                subscriber.onError(t);
            }
        }
    }

  
    
   
    private static class OnSubscribe<T> extends SubscriberNotifier.Notification<T> {
        private final Subscription subscription;
        
        public OnSubscribe(Subscription subscription) {
            this.subscription = subscription;
        }
        
        @Override
        public void sendTo(Subscriber<? super T> subscriber) {
            subscriber.onSubscribe(subscription);
        }
    }
    
    
  
    private static class OnNext<T> extends SubscriberNotifier.Notification<T> {
        private final T t;
        
        public OnNext(T t) {
            this.t = t;
        }
        
        @Override
        public void sendTo(Subscriber<? super T> subscriber) {
            subscriber.onNext(t);
        }
        
        @Override
        public String toString() {
            return t.toString();
        }
    }
    
    
   
    
    private static class OnError<T> extends SubscriberNotifier.TerminatingNotification<T> {
        private final Throwable error;
        
        public OnError(Throwable error) {
            this.error = error;
        }
        
        @Override
        public void sendTo(Subscriber<? super T> subscriber) {
            subscriber.onError(error);
        }
    }
    
    
    private static class OnComplete<T> extends SubscriberNotifier.TerminatingNotification<T> {
        
        @Override
        public void sendTo(Subscriber<? super T> subscriber) {
            subscriber.onComplete();
        }
    }
    
  
    
    
    private static abstract class Notification<T> { 
        
        abstract void sendTo(Subscriber<? super T> subscriber);
        
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
