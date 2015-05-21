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





import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;


import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;



/**
 * IteratorToPublisherAdapter
 *
 * @param <T> the element type
 */
class IteratorToPublisherAdapter<T> implements Publisher<T> {
    private boolean subscribed = false; // true after first subscribe
    private final Iterator<T> it;

    /**
     * @param it the underlying iterator 
     */
    public IteratorToPublisherAdapter(Iterator<T> it) {
        this.it = it;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        synchronized (this) {
            if (subscribed == true) {
                subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
            } else {
                subscribed = true;
                subscriber.onSubscribe(new IteratorbasedSubscription<>(it, subscriber));
            }
        }
    }
    
    
    
    private static final class IteratorbasedSubscription<T> implements Subscription {
        private AtomicBoolean isOpen = new AtomicBoolean(true);
        private final Subscriber<? super T> subscriber;
        private final Iterator<T> it;

        
        private IteratorbasedSubscription(Iterator<T> it, Subscriber<? super T> subscriber) {
            this.it = it;
            this.subscriber = subscriber;
        }
        
        
        @Override
        public void request(long n) {
            for (int i = 0; i < n; i++) {
                request();
            }
        }
        
        private void request() {
            try {
                if (it.hasNext()) {
                    subscriber.onNext(it.next());
                } else {
                    cancel();
                }
            } catch (RuntimeException rt) {
                if (isOpen.getAndSet(false)) {
                    subscriber.onError(rt);
                }
            }
        }

        @Override
        public void cancel() {
            if (isOpen.getAndSet(false)) {
                subscriber.onComplete();
            }
        }
    }
}