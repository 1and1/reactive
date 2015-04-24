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
package net.oneandone.reactive.pipe;





import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;




class ForwardingProcessor<T, R> implements Processor<T, R>, Subscription {
    private final ForwardingSubscription forwardingSubscription = new ForwardingSubscription();
    private final Function<? super T,  CompletableFuture<? extends R>> mapper;
    private final AtomicReference<Optional<Subscriber<? super R>>> sinkSubscriberRef = new AtomicReference<>(Optional.empty());
    
    
    public ForwardingProcessor(Publisher<T> source, Function<? super T, CompletableFuture<? extends R>> mapper) {
        this.mapper = mapper;
        source.subscribe(this);
    }
    
    
    /////////////////////////
    //  consumes the source
    
    @Override
    public void onSubscribe(Subscription subscription) {
        synchronized (this) {
            forwardingSubscription.setSubscription(subscription);
        }
    }
    
    @Override
    public void onNext(T element) {
        sinkSubscriberRef.get().ifPresent(subscriber -> handleElement(subscriber, element));
    }

    private void handleElement(Subscriber<? super R> subscriber, T element) {
        mapper.apply(element)
              .whenCompleteAsync((result , error) -> {
                                                          if (error == null) {
                                                              subscriber.onNext(result);
                                                          } else {
                                                              subscriber.onError(error);
                                                          }
                  
                                                     });
    }
    
    
    @Override
    public void onError(Throwable t) {
        sinkSubscriberRef.get().ifPresent(subscriber -> subscriber.onError(t));
    }
    
    @Override
    public void onComplete() {
        sinkSubscriberRef.get().ifPresent(subscription -> subscription.onComplete());
    }
    
    
      
    /////////////////////////////////////
    // handle the sink 
    
    @Override
    public void subscribe(Subscriber<? super R> subscriber) {
        sinkSubscriberRef.set(Optional.of(subscriber));
        subscriber.onSubscribe(this);
    }
    
    @Override
    public void request(long n) {
        // Subscription: MUST NOT allow unbounded recursion such as Subscriber.onNext -> Subscription.request -> Subscriber.onNext
        ForkJoinPool.commonPool().execute(() -> forwardingSubscription.request(n));
    }
            
    @Override
    public void cancel() {
        forwardingSubscription.cancel();
    }
    
 
    private static final class ForwardingSubscription implements Subscription {
        private long pendingRequests = 0;
        private boolean pendingCancel = false; 
        private Subscription sourceSubscription = null;
        
        
        void setSubscription(Subscription sourceSubscription) {
            synchronized (this) {
                this.sourceSubscription = sourceSubscription;
                if (pendingRequests > 0) {
                    sourceSubscription.request(pendingRequests);
                    pendingRequests = 0;
                }
                
                if (pendingCancel) {
                    sourceSubscription.cancel();
                    pendingCancel = false;
                }
            }
        }
        
        @Override
        public void request(long n) {
            synchronized (this) {
                if (sourceSubscription == null) {
                    pendingRequests += n;
                } else {
                    sourceSubscription.request(n);
                }
            }
        }
         
        @Override
        public void cancel() {
            synchronized (this) {
                if (sourceSubscription == null) {
                    pendingCancel = true;
                } else {
                    sourceSubscription.cancel();
                }
            }
        }
    }
}