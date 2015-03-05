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
package net.oneandone.reactive.rest;



import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;

import net.oneandone.reactive.rest.container.ResultSubscriber;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.Lists;



  
@Path("/Publisher")
public class PublisherResource {
    
    
    
    @Path("/first")
    @GET
    public void retrievePublisherFirstAsync(@QueryParam("num") int num, @Suspended AsyncResponse resp) {    
        
        TestPublisher<String> publisher = new TestPublisher<>();
        
        new Thread() {
            public void run() {
                
                if (num < 0) {
                    pause(100);
                    publisher.pushError(new IOException());
                } else {
                    for (int i = 0; i < num; i++) {
                        pause(100);
                        publisher.push(Integer.toString(i + 1));
                    }
                    publisher.close();
                }
            };
            
        }.start();

        publisher.subscribe(ResultSubscriber.toConsumeFirstSubscriber(resp));
    }

    
    
    @Path("/single")
    @GET
    public void retrieveSingle(@QueryParam("num") int num, @Suspended AsyncResponse resp) {
     
        TestPublisher<String> publisher = new TestPublisher<>();
        
        new Thread() {
            public void run() {
                
                if (num < 0) {
                    pause(100);
                    publisher.pushError(new IOException());
                } else {
                    for (int i = 0; i < num; i++) {
                        pause(100);
                        publisher.push(Integer.toString(i + 1));
                    }
                    publisher.close();
                }
            };
            
        }.start();

        publisher.subscribe(ResultSubscriber.toConsumeSingleSubscriber(resp));
    }

    
    
    private void pause(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) { }
    }
    
    
    private static final class TestPublisher<T> implements Publisher<T> {
        private final AtomicReference<Optional<SimpleSubscription>> subscriptionRef = new AtomicReference<>(Optional.empty());
        private final AtomicBoolean moreDataExepected = new AtomicBoolean(true);
        
        public void push(T element) {
            subscriptionRef.get().ifPresent(subscription -> subscription.process(element));
        }

        public void pushError(Throwable t) {
            subscriptionRef.get().ifPresent(subscription -> subscription.teminateWithError(t));
        }

        
        public void close() {
            moreDataExepected.set(false);
            subscriptionRef.get().ifPresent(subscription -> subscription.process());
        }
        
        @Override
        public void subscribe(Subscriber<? super T> subscriber) {
            SimpleSubscription subscription = new SimpleSubscription(subscriber);
            this.subscriptionRef.set(Optional.of(subscription));
            subscriber.onSubscribe(subscription);
        }
        
        
        private final class SimpleSubscription implements Subscription {
            private AtomicBoolean isOpen = new AtomicBoolean(true);
            private AtomicLong numRequested = new AtomicLong();
            private final Subscriber<? super T> subscriber;
            
            private final List<T> elements = Lists.newArrayList();
            
            
            public SimpleSubscription(Subscriber<? super T> subscriber) {
                this.subscriber = subscriber;
            }
            
            @Override
            public void request(long n) {
                numRequested.addAndGet(n);
                process();
            }
            
            
            @Override
            public void cancel() {
                terminateRegularly();
            }
            

            private void process() {
                process(null);
            }
            
            private void process(T element) {
                synchronized (this) {
                    if (element != null) {
                        elements.add(element);
                    }
                    
                    
                    if (elements.isEmpty() && !moreDataExepected.get()) {
                        cancel();
                    }
                    
                    while (isOpen.get() && (numRequested.get() > 0) && (!elements.isEmpty())) {
                        try {
                            subscriber.onNext(elements.remove(0));
                        } finally {
                            numRequested.decrementAndGet();
                        }
                    }
                }
            }
            
            
            private void terminateRegularly() {
                if (isOpen.getAndSet(false)) {
                    subscriber.onComplete();
                }
            }
            
            
            public void teminateWithError(Throwable t) {
                if (isOpen.getAndSet(false)) {
                    subscriber.onError(t);
                }
            }
        }
    }
}