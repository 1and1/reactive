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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.servlet.ServletInputStream;

import net.oneandone.reactive.sse.ServerSentEvent;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.Queues;


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
            if (subscribed == true) {
                subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
            } else {
                subscribed = true;
                subscriber.onSubscribe(new SEEEventReaderSubscription(inputStream, subscriber));
            }
        }
    }

    
    
    private static class SEEEventReaderSubscription implements Subscription {
        private final SubscriberNotifier subscriberNotifier;
        private final SseReadableChannel channel;


        public SEEEventReaderSubscription(ServletInputStream inputStream, Subscriber<? super ServerSentEvent> subscriber) {
            Executor executor = Executors.newCachedThreadPool();

            this.subscriberNotifier = new SubscriberNotifier(executor, subscriber);
            this.channel = new SseReadableChannel(inputStream,                                     // servlet input stream
                                                  event -> emitNotification(new OnNext(event)),    // event consumer
                                                  error -> emitNotification(new OnError(error)),   // error consumer
                                                  Void -> emitNotification(new OnComplete()));     // completion consumer         
            
            subscriberNotifier.emitNotification(new OnSubscribe());
        }

        
        private void emitNotification(SubscriberNotifier.Notification notification) {
            subscriberNotifier.emitNotification(notification);
        }

        
        @Override
        public void cancel() {
            channel.close();
        }
        
        
        @Override
        public void request(long n) {
            if(n <= 0) {
                // https://github.com/reactive-streams/reactive-streams#3.9
                subscriberNotifier.emitNotification(new OnError(new IllegalArgumentException("Non-negative number of elements must be requested: https://github.com/reactive-streams/reactive-streams#3.9")));
            } else {
                channel.request(n);
            }
        }
        
        
        
        
        private class OnSubscribe extends SubscriberNotifier.Notification {
            
            @Override
            public void signalTo(Subscriber<? super ServerSentEvent> subscriber) {
                subscriber.onSubscribe(SEEEventReaderSubscription.this);
            }
        }
        
        
        private class OnNext extends SubscriberNotifier.Notification {
            private final ServerSentEvent event;
            
            public OnNext(ServerSentEvent event) {
                this.event = event;
            }
            
            @Override
            public void signalTo(Subscriber<? super ServerSentEvent> subscriber) {
                subscriber.onNext(event);
            }
        }
        
        
        
        private class OnError extends SubscriberNotifier.TerminatingNotification {
            private final Throwable error;
            
            public OnError(Throwable error) {
                this.error = error;
            }
            
            @Override
            public void signalTo(Subscriber<? super ServerSentEvent> subscriber) {
                subscriber.onError(error);
            }
        }
        

        private class OnComplete extends SubscriberNotifier.TerminatingNotification {
            
            @Override
            public void signalTo(Subscriber<? super ServerSentEvent> subscriber) {
                subscriber.onComplete();
            }
        }


        private static final class SubscriberNotifier implements Runnable {
            private final ConcurrentLinkedQueue<Notification> notifications = Queues.newConcurrentLinkedQueue();
            private final Executor executor;
            private final AtomicBoolean isOpen = new AtomicBoolean(true);
            
            private final Subscriber<? super ServerSentEvent> subscriber;
            
            public SubscriberNotifier(Executor executor, Subscriber<? super ServerSentEvent> subscriber) {
                this.executor = executor;
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
                    executor.execute(this);
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
          
            
            
            private static abstract class Notification { 
                
                abstract void signalTo(Subscriber<? super ServerSentEvent> subscriber);
                
                boolean isTerminating() {
                    return false;
                }
            };
            
            
            
            // Once a terminal state has been signaled (onError, onComplete) it is REQUIRED that no further signals occur
            private static abstract class TerminatingNotification extends SubscriberNotifier.Notification { 
                
                boolean isTerminating() {
                    return true;
                }
            };

        }
    }
}        
    

