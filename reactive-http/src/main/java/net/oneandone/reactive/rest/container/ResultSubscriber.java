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



import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.container.AsyncResponse;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;




/**
 * ResultSubscriber
 *
 */
public class ResultSubscriber {
    

    /**
     * maps the asyn HTTP response to a Subscriber which writes the first element into the HTTP response. 
     * If no element is available, a 204 No Content response is returned
     *  
     * @param asyncResponse   the async response 
     * @return the subscriber
     */
    public static final <T> Subscriber<T> toConsumeFirstSubscriber(AsyncResponse asyncResponse) {
        return new FirstSubscriber<T>(asyncResponse);
    }


    private static class FirstSubscriber<T> implements Subscriber<T> {
        
        private final AsyncResponse asyncResponse;
        private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>(); 
        
        private FirstSubscriber(AsyncResponse asyncResponse) {
            this.asyncResponse = asyncResponse;
        }
        
        @Override
        public void onSubscribe(Subscription subscription) {
            subscriptionRef.set(subscription);
            subscription.request(1);
        }
        
        @Override
        public void onError(Throwable error) {
            error = Throwables.unwrapIfNecessary(error, 10);
            asyncResponse.resume(error);
        }
        
        @Override
        public void onNext(T element) {
            asyncResponse.resume(element);
            subscriptionRef.get().cancel();
        }
        
        @Override
        public void onComplete() {
            if (!asyncResponse.isDone()) {
                asyncResponse.resume(Response.noContent().build());
            }
        }
    }
    
    
    /**
     *  maps the asyn HTTP response to a Subscriber which writes the first element into the HTTP response.
     *  If no element is available, a 404 response is returned. If more than 1 element is available a 
     *  409 Conflict response is returned  
     *  
     * @param asyncResponse   the async response 
     * @return the subscriber 
     */
    public static final <T> Subscriber<T> toConsumeSingleSubscriber(AsyncResponse asyncResponse) {
        return new SingleSubscriber<T>(asyncResponse);
    }


    private static class SingleSubscriber<T> implements Subscriber<T> {
        
        private final AsyncResponse asyncResponse;
        private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
        private final AtomicReference<T> elementRef = new AtomicReference<>();
        
        
        private SingleSubscriber(AsyncResponse asyncResponse) {
            this.asyncResponse = asyncResponse;
        }
        
        @Override
        public void onSubscribe(Subscription subscription) {
            subscriptionRef.set(subscription);
            subscription.request(1);
        }
        
        @Override
        public void onError(Throwable error) {
            error = Throwables.unwrapIfNecessary(error, 10);
            asyncResponse.resume(error);
        }
        
        @Override
        public void onNext(T element) {
            if (elementRef.getAndSet(element) == null) {
                subscriptionRef.get().request(1);
                
            } else {
                // more than 1 element causes a conflict exception
                onError(new ClientErrorException(Status.CONFLICT));
            }
        }
        
        @Override
        public void onComplete() {
            if (!asyncResponse.isDone()) {
                T element = elementRef.get();
                if (element == null) {
                    asyncResponse.resume(new NotFoundException());
                } else {
                    asyncResponse.resume(element);
                }
            }
        }
    }
}