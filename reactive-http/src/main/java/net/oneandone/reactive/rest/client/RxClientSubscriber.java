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
package net.oneandone.reactive.rest.client;



import java.net.URI;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.ServerErrorException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


import net.oneandone.reactive.utils.IllegalStateSubscription;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;



public class RxClientSubscriber<T> implements Subscriber<T> {
    private static final Logger LOG = LoggerFactory.getLogger(RxClientSubscriber.class);
    
    private final String id = UUID.randomUUID().toString();
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    
    
    // properties
    private final RxClient client;
    private final URI uri;
    private final MediaType mediaType;
    private final String method;
    private final boolean isAutoRetry;
    private final int maxInFlight; 

    
    private final AtomicBoolean isSubscribed = new AtomicBoolean(false); 
    private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>(new IllegalStateSubscription());
    
    
    // statistics
    private final AtomicLong numSent = new AtomicLong();
    private final AtomicLong numSendErrors = new AtomicLong();
    private final AtomicLong numResendTrials = new AtomicLong();
     
    // statistics
    private final Set<Object> inFlight = Collections.synchronizedSet(Sets.newHashSet());
    
    
    
    public RxClientSubscriber(Client client, URI uri) {
        this(client,
             uri, 
             "POST",
             MediaType.APPLICATION_JSON_TYPE,
             25,
             true);
    }

    private RxClientSubscriber(Client client,
                               URI uri, 
                               String method,
                               MediaType mediaType,
                               int prefetchSize,
                               boolean isAutoRetry) {
        this.client = new RxClient(client); 
        this.uri = uri;
        this.method = method;
        this.mediaType = mediaType;
        this.maxInFlight = prefetchSize;
        this.isAutoRetry = isAutoRetry;
    }
    
    
    /**
     * @param mediaType the media type to use
     * @return a new instance with the updated behavior
     */
    public RxClientSubscriber<T> mediaType(MediaType mediaType) {
        return new RxClientSubscriber<>(this.client,
                                        this.uri, 
                                        this.method,
                                        this.mediaType,
                                        this.maxInFlight,
                                        this.isAutoRetry);
    }
    
    
    /**
     * @return a new instance with the updated behavior
     */
    public RxClientSubscriber<T> usePost() {
        return new RxClientSubscriber<>(this.client,
                                        this.uri, 
                                        "POST",
                                        this.mediaType,
                                        this.maxInFlight,
                                        this.isAutoRetry);
    }

    /**
     * @return a new instance with the updated behavior
     */
    public RxClientSubscriber<T> usePut() {
        return new RxClientSubscriber<>(this.client,
                                        this.uri, 
                                        "PUT",
                                        this.mediaType,
                                        this.maxInFlight,
                                        this.isAutoRetry);
    }

    
    /**
     * @param maxInFlight  the max number of in flight transactions
     * @return a new instance with the updated behavior
     */
    public RxClientSubscriber<T> maxInFlight(int maxInFlight) {
        return new RxClientSubscriber<>(this.client,
                                        this.uri, 
                                        this.method,
                                        this.mediaType,
                                        maxInFlight,
                                        this.isAutoRetry);
    }
     


    
    /**
     * @param isAutoRetry if failed send activity should be retried
     * @return a new instance with the updated behavior
     */
    public RxClientSubscriber<T> autoRetry(boolean isAutoRetry) {
        return new RxClientSubscriber<>(this.client,
                                        this.uri, 
                                        this.method,
                                        this.mediaType,
                                        this.maxInFlight,
                                        isAutoRetry);
    }
    
    

    @Override
    public void onSubscribe(Subscription subscription) {
        if (isSubscribed.getAndSet(true)) {
            throw new IllegalStateException("already subscribed");
            
        } else {
            subscriptionRef.set(subscription);
            subscription.request(maxInFlight);
        }
    }

    
    @Override
    public void onNext(T element) {

        if (isOpen.get()) {
            final Object marker = new Object();
            inFlight.add(marker);
            
            send(element).thenAccept((response) -> {
                                                     inFlight.remove(marker);
                                                     response.close();
                                                     LOG.debug("[" + id + "] element transmitted");
                                                     numSent.incrementAndGet();
                                                     subscriptionRef.get().request(1); 
                                                   })
                         .exceptionally((error -> {
                                                     inFlight.remove(marker);
                                                     numSendErrors.incrementAndGet();
                                                     if (isAutoRetry) {
                                                         LOG.debug("[" + id + "] " + error.getMessage() + " error occured by transmitting element " + element + " retry sending");
                                                         numResendTrials.incrementAndGet();
                                                         
                                                         // TODO add with delay 
                                                         onNext(element);
                                                         
                                                     } else { 
                                                         LOG.debug("[" + id + "] " + error.getMessage() + " error occured by transmitting element " + element + " terminating sink");
                                                         subscriptionRef.get().cancel();
                                                     }
                                                     
                                                     return null;
                                                  }));     
        }
    }
    

    
    private CompletableFuture<Response> send(T element) {
        
        try {
            if (method.equalsIgnoreCase("POST")) {
                return client.target(uri)
                             .request()
                             .rx()
                             .post(Entity.entity(element, mediaType));
            } else {
                return client.target(uri)
                             .request()
                             .rx()
                             .put(Entity.entity(element, mediaType));

            }
            
        } catch (ServerErrorException error) {
            CompletableFuture<Response> errorPromise = new CompletableFuture<>();
            errorPromise.completeExceptionally(error);
            return errorPromise;
            
        } catch (RuntimeException rt) {
            throw rt;
        }
    }
    
    
    @Override
    public void onError(Throwable error) {
        LOG.debug("[" + id + "] " + error.getMessage() + " error occured " + error.toString());
    }
    
    @Override
    public void onComplete() {
        LOG.debug("[" + id + "] closed");
    }
    

    public void shutdown() {
        subscriptionRef.get().cancel();
    }
    
    public void shutdownNow() {
        isOpen.set(false);
        shutdown();
    }
    
    
    @Override
    public String toString() {
       return  id + ", inFlight: " + inFlight.size() + " , numSent: " + numSent.get() + ", numSendErrors: " + numSendErrors + ", numResendTrials: " + numResendTrials;
    }
}