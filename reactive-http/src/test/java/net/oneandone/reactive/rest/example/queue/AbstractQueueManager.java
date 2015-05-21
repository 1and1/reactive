/*
 * ProcessingQueueActiveMQImpl.java 21.11.2014
 *
 * Copyright (c) 2014 1&1 Internet AG. All rights reserved.
 *
 * $Id$
 */
package net.oneandone.reactive.rest.example.queue;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import net.oneandone.reactive.utils.SubscriberNotifier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;



abstract class AbstractQueueManager<T> implements QueueManager<T> {
   
    
    @Override
    public Subscriber<Message<T>> newSubscriber(String queueName) {
        return new QueueSubscriber(queueName);
    }
    
    
    
    private final class QueueSubscriber implements Subscriber<Message<T>> {
        private final MessageSink<T> consumer;
        private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
        
        public QueueSubscriber(String queueName) {
            this.consumer = newConsumer(queueName);
        }
        
        @Override
        public void onSubscribe(Subscription subscription) {
            subscriptionRef.set(subscription);
            subscription.request(50);
        }
        
        @Override
        public void onNext(Message<T> message) {
            try {
                consumer.accept(message);
            } finally {
                subscriptionRef.get().request(1);
            }
        }
        
        @Override
        public void onComplete() {
            try {
                consumer.close();
            } catch (IOException igone) { }
        }
        
        @Override
        public void onError(Throwable t) {
            try {
                consumer.close();
            } catch (IOException igone) { }
        }
    }
    
    
    protected static interface MessageSink<T> extends Consumer<Message<T>>, Closeable { 
        
    }
    
    
    abstract protected MessageSink<T> newConsumer(String queueName);

    
    
    
    
    
    @Override
    public Publisher<Message<T>> newPublisher(String queueName) {
        return new QueuePublisher(queueName);
    }
  
    
    
    
    private final class QueuePublisher implements Publisher<Message<T>> {
        private final String queuename;
        
        public QueuePublisher(String queuename) {
            this.queuename = queuename;
        }
        
        @Override
        public void subscribe(Subscriber<? super Message<T>> subscriber) {
            // https://github.com/reactive-streams/reactive-streams-jvm#1.9
            if (subscriber == null) {  
                throw new NullPointerException("subscriber is null");
            }

            new QueueSubscription(subscriber, queuename).init();
        }
         
        
        private class QueueSubscription implements Subscription, Consumer<Message<T>> {  
            private final AtomicBoolean isOpen = new AtomicBoolean(true);
            private final AtomicLong numRequested = new AtomicLong(); 
            
            private final SubscriberNotifier<Message<T>> notifier;
            private final MessageSource<T> messageSource;
            
            
            public QueueSubscription(Subscriber<? super Message<T>> subscriber, String queueName) {
                notifier = new SubscriberNotifier<>(subscriber, this);
                messageSource = newMessageSource(queueName, this);
            }
            
            void init() {
                notifier.start();
            }

            
            @Override
            public void cancel() {
                isOpen.set(false);      
                notifier.notifyOnComplete();
                try {
                    messageSource.close();
                } catch (IOException ignore) { }
            }

            @Override
            public void request(long n) {
                numRequested.addAndGet(n);
                controlFlow();
            }
            
            @Override
            public void accept(Message<T> message) {
                notifier.notifyOnNext(message);
                numRequested.decrementAndGet();
                controlFlow();
            }
            
            private void controlFlow() {
                if (numRequested.get() > 0) {
                    messageSource.resume();
                } else {
                    messageSource.suspend();
                }
            }
            
        }
    }
    
    
    
    
    
    abstract protected MessageSource<T> newMessageSource(String queueName, Consumer<Message<T>> consumer);

    
    protected static interface MessageSource<T> extends Closeable { 
      
        void suspend();
        
        void resume();
    }

}
