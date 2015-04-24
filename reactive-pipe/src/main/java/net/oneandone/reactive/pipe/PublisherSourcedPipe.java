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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;




/**
 * Publisher-based pipe implementation 
 * @param <T> the element type
 */
class PublisherSourcedPipe<T> implements Pipe<T> {
    private final Publisher<T> publisher;
    

    /**
     * @param publisher the publisher
     */
    public PublisherSourcedPipe(Publisher<T> publisher) {
        this.publisher = publisher;
    }

    
    @Override
    public void consume(Subscriber<? super T> subscriber) {
        publisher.subscribe(subscriber);
    }
    
    @Override
    public void consume(Consumer<? super T> consumer) {
        consume(consumer, error -> { });
    }
    
    
    @Override
    public void consume(Consumer<? super T> consumer, Consumer<? super Throwable> errorConsumer) {
        consume(consumer, errorConsumer, Void -> { });
    }
    
    @Override
    public void consume(Consumer<? super T> consumer, Consumer<? super Throwable> errorConsumer, Consumer<Void> completeConsumer) {
        consume(new ConsumerAdapter<>(consumer, errorConsumer, completeConsumer));        
    }
    
    private static final class ConsumerAdapter<E> implements Subscriber<E> {
        private final Consumer<? super E> consumer;
        private final Consumer<? super Throwable> errorConsumer;
        private final Consumer<Void> completeConsumer;
        private final AtomicReference<Optional<Subscription>> subscriptionRef = new AtomicReference<>(Optional.empty()); 
        
        
        public ConsumerAdapter(Consumer<? super E> consumer, Consumer<? super Throwable> errorConsumer, Consumer<Void> completeConsumer) {
            this.consumer = consumer;
            this.errorConsumer = errorConsumer;
            this.completeConsumer = completeConsumer;
        }
        
        @Override
        public void onSubscribe(Subscription subscription) {
            subscriptionRef.set(Optional.of(subscription));
            subscription.request(1);
        }
        
        @Override
        public void onNext(E element) {
            try {
                consumer.accept(element);
                subscriptionRef.get().ifPresent(subscription -> subscription.request(1));
            } catch (RuntimeException rt) {
                onError(rt);
            }
        }

        @Override
        public void onError(Throwable t) {
            try {
                errorConsumer.accept(t);
            } finally {
                subscriptionRef.get().ifPresent(subscription -> subscription.cancel());
            }
        }
        
        @Override
        public void onComplete() {
            completeConsumer.accept(null);
        }
    }
    
    
    @Override
    public <V> PublisherSourcedPipe<V> map(Function<? super T, ? extends V> fn) {
        return new PublisherSourcedPipe<>(new ForwardingProcessor<T, V>(publisher, in -> CompletableFuture.completedFuture(fn.apply(in))));
    }
    
    
    
    @Override
    public <V> Pipe<V> flatMap(Function<? super T, CompletableFuture<? extends V>> fn) {
        return new PublisherSourcedPipe<>(new ForwardingProcessor<T, V>(publisher, fn));
    }
    
  
    
    @Override
    public Pipe<T> filter(Predicate<? super T> predicate) {
        return new PublisherSourcedPipe<>(new FilteringProcessor<T>(publisher, predicate));
    }
 
    private static final class FilteringProcessor<T> extends ForwardingProcessor<T, T> {
        private final Predicate<? super T> predicate;
        
        public FilteringProcessor(Publisher<T> source, Predicate<? super T> predicate) {
            super(source, element -> CompletableFuture.completedFuture(element));
            this.predicate = predicate;
        }
        
        @Override
        public void onNext(T element) {
            if (predicate.test(element)) {
                super.onNext(element);
            } else {
                request(1);
            }
        }
    }
    
    
    
    @Override
    public Pipe<T> skip(long n) {
        return new PublisherSourcedPipe<>(new SkippingProcessor<T>(publisher, n));
    }
 
    private static final class SkippingProcessor<T> extends ForwardingProcessor<T, T> {
        private final long numToSkip;
        private long numProcessed;
        
        public SkippingProcessor(Publisher<T> source, long numToSkip) {
            super(source, element -> CompletableFuture.completedFuture(element));
            this.numToSkip = numToSkip;
        }
        
        @Override
        public void onNext(T element) {
            numProcessed++;
            if (numToSkip >= numProcessed) {
                request(1);
            } else {
                super.onNext(element);
            }            
        }
    }
    
    
    @Override
    public Pipe<T> limit(long maxSize) {
        return new PublisherSourcedPipe<>(new LimittingPublisher<T>(publisher, maxSize));
    }
 
    private static final class LimittingPublisher<T> extends ForwardingProcessor<T, T> {
        private final long max;
        private long numProcessed;
        
        public LimittingPublisher(Publisher<T> source, long max) {
            super(source, element -> CompletableFuture.completedFuture(element));
            this.max = max;
        }
        
        @Override
        public void onNext(T element) {
            numProcessed++;
            if (numProcessed > max) {
                cancel();
            } else {
                super.onNext(element);
            }            
        }
    }
}