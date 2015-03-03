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



import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.container.AsyncResponse;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;




/**
 * PublisherConsumer
 *
 */
public class PublisherConsumer<T> implements BiConsumer<Publisher<T>, Throwable> {
    
    private final AsyncResponse asyncResponse;
    
    private PublisherConsumer(AsyncResponse asyncResponse) {
        this.asyncResponse = asyncResponse;
    }

    
    /**
     * forwards the response to the REST response object. Includes error handling also 
     * @param asyncResponse the REST response
     * @return the BiConsumer consuming the response/error pair
     */
    public static final <T> BiConsumer<Publisher<T>, Throwable> writeSingleTo(AsyncResponse asyncResponse) {
        return new PublisherConsumer<T>(asyncResponse);
    }
    
    
    @Override
    public void accept(Publisher<T> publisher, Throwable error) {
        SingleEntityResponseSubscriber<T> subscriber = new SingleEntityResponseSubscriber<>(asyncResponse);
        if (error != null) {
            subscriber.onError(error);
        } else {
            publisher.subscribe(subscriber);
        }
    }
    

    private static boolean isCompletionException(Throwable t) {
        return CompletionException.class.isAssignableFrom(t.getClass());
    }

    
    private static class SingleEntityResponseSubscriber<T> implements Subscriber<T> {
          private final AtomicBoolean isOpen = new AtomicBoolean(true);
          private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
                  
          private final AtomicBoolean isResponseProcessed = new AtomicBoolean();
          private final AsyncResponse response;
          
          
          public SingleEntityResponseSubscriber(AsyncResponse response) {
              this.response = response;
          }   

          @Override
          public void onSubscribe(Subscription subscription) {
              subscriptionRef.set(subscription);
              subscriptionRef.get().request(1); 
          }

          @Override
          public void onNext(T element) {
              isResponseProcessed.set(true);
              response.resume(element);
          }

          @Override
          public void onError(Throwable t) {
              isResponseProcessed.set(true);
              t = unwrapIfNecessary(t, 10);
              response.resume(t);
          }
        
          
          private static Throwable unwrapIfNecessary(Throwable ex, int maxDepth)  {
              if (isCompletionException(ex)) {
                  Throwable e = ex.getCause();
                  if (e != null) {
                      if (maxDepth > 1) {
                          return unwrapIfNecessary(e, maxDepth - 1);
                      } else {
                          return e;
                      }
                  }
              }
                  
              return ex;
          }
          
          @Override
          public void onComplete() {
              if (!isResponseProcessed.get()) {
                  onError(new NotFoundException());
              }
              close();
          }

          private void close() {
              if (isOpen.getAndSet(false)) {
                  subscriptionRef.get().cancel();
              }
          }
      }
}