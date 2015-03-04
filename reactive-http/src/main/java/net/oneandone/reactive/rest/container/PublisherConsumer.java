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
package net.oneandone.reactive.rest.container;



import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.container.AsyncResponse;





import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;




/**
 * PublisherConsumer
 *
 */
public class PublisherConsumer{
    

    /**
     * reads the first element of the publisher stream and write it into the response. If no element is available, a 204 No Content response is returned 
     * @param asyncResponse   the async response 
     * @return the consumer 
     */
    public static final <T> BiConsumer<Publisher<T>, Throwable> writeFirstTo(AsyncResponse asyncResponse) {
        return new FirstPublisherConsumer<T>(asyncResponse);
    }

    
    /**
     * reads the first element of the publisher stream and write it into the response. If no element is available, a 404 response is returned.
     * If more than 1 element is available a 409 Conflict response is returned  
     * @param asyncResponse   the async response 
     * @return the consumer 
     */
    public static final <T> BiConsumer<Publisher<T>, Throwable> writeSingleTo(AsyncResponse asyncResponse) {
        return new SinglePublisherConsumer<T>(asyncResponse);
    }

    
    private static class FirstPublisherConsumer<T> implements BiConsumer<Publisher<T>, Throwable> {
        private final AsyncResponse asyncResponse;
        
        private FirstPublisherConsumer(AsyncResponse asyncResponse) {
            this.asyncResponse = asyncResponse;
        }
        
        @Override
        public void accept(Publisher<T> publisher, Throwable error) {
            Subscriber<T> subscriber = new FirstEntityResponseSubscriber<>(asyncResponse);
            if (error != null) {
                subscriber.onError(error);
            } else {
                publisher.subscribe(subscriber);
            }
        }
    }

    private static class SinglePublisherConsumer<T> implements BiConsumer<Publisher<T>, Throwable> {
        private final AsyncResponse asyncResponse;
        
        private SinglePublisherConsumer(AsyncResponse asyncResponse) {
            this.asyncResponse = asyncResponse;
        }
        
        @Override
        public void accept(Publisher<T> publisher, Throwable error) {
            Subscriber<T> subscriber = new SingleEntityResponseSubscriber<>(asyncResponse);
            if (error != null) {
                subscriber.onError(error);
            } else {
                publisher.subscribe(subscriber);
            }
        }
    }


    
    private static class FirstEntityResponseSubscriber<T> implements Subscriber<T> {
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        private final AtomicReference<Optional<Subscription>> subscriptionRef = new AtomicReference<>(Optional.empty());
                
        private final AtomicBoolean isResponseProcessed = new AtomicBoolean();
        private final AsyncResponse response;
        
        
        public FirstEntityResponseSubscriber(AsyncResponse response) {
            this.response = response;
        }   

        @Override
        public void onSubscribe(Subscription subscription) {
            subscriptionRef.set(Optional.of(subscription));
            requestNext();
        }
        
        
        protected void requestNext() {
            subscriptionRef.get().ifPresent(subscription -> subscription.request(1));
        }

        @Override
        public void onNext(T element) {
            if (!isResponseProcessed.getAndSet(true)) {
                response.resume(element);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (!isResponseProcessed.getAndSet(true)) {
                t = Throwables.unwrapIfNecessary(t, 10);
                response.resume(t);
            }
        }
      
        
        @Override
        public void onComplete() {
            // if no response is send, return 204 
            if (!isResponseProcessed.getAndSet(true)) {
                response.resume(Response.noContent().build());
            }
            
            closeSubscription();
        }
        
        protected void closeSubscription() {
            if (isOpen.getAndSet(false)) {
                subscriptionRef.get().ifPresent(subscription -> subscription.cancel());
            }
        }
    }
    
    
    
    private static class SingleEntityResponseSubscriber<T> extends FirstEntityResponseSubscriber<T> {
        private final AtomicReference<T> elementRef = new AtomicReference<>();
        
        public SingleEntityResponseSubscriber(AsyncResponse response) {
            super(response);
        }   

        @Override
        public void onNext(T element) {
            if (elementRef.getAndSet(element) == null) {
                requestNext();
                
            } else {
                // more than 1 element causes a conflict exception
                onError(new ClientErrorException(Status.CONFLICT));
            }
        }
        
        @Override
        public void onComplete() {
            T element = elementRef.get();
            if (element == null) {
                super.onError(new NotFoundException());
            } else {
                super.onNext(element);
            }
            
            closeSubscription();
        }
    }
}