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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Queues;

import net.oneandone.reactive.sse.ServerSentEvent;

import org.reactivestreams.Subscriber;



final class SubscriberNotifier implements Runnable {
    private final ConcurrentLinkedQueue<Notification> notifications = Queues.newConcurrentLinkedQueue();
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    
    private final Subscriber<? super ServerSentEvent> subscriber;
    
    public SubscriberNotifier(Subscriber<? super ServerSentEvent> subscriber) {
        this.subscriber = subscriber;
    }
    
    private void close() {
        isOpen.set(false);
        notifications.clear();  
    }
    
    public void emitNotification(Notification notification) {
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
                    Notification notification = notifications.poll(); 
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
  
    
    
    static abstract class Notification { 
        
        abstract void signalTo(Subscriber<? super ServerSentEvent> subscriber);
        
        boolean isTerminating() {
            return false;
        }
    };
    
    
    
    // Once a terminal state has been signaled (onError, onComplete) it is REQUIRED that no further signals occur
    static abstract class TerminatingNotification extends SubscriberNotifier.Notification { 
        
        boolean isTerminating() {
            return true;
        }
    };
}
