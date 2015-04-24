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




import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import net.oneandone.reactive.pipe.Pipes;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;


public class PublisherBasedPipeTest {
  
  
    @Test
    public void testPlain() throws Exception {
        TestPublisher<String> publisher = new TestPublisher<>();
        TestSubscriber<String> subscriber = new TestSubscriber<>();
        
        
        Pipes.newPipe(publisher)
             .consume(subscriber);
        
        publisher.push("1");
        publisher.push("2");
        publisher.push("3");
        publisher.completed();
        

        
        UnmodifiableIterator<String> elements = subscriber.getElements().iterator();
        Assert.assertEquals("1", elements.next());
        Assert.assertEquals("2", elements.next());
        Assert.assertEquals("3", elements.next());
        Assert.assertFalse(elements.hasNext());
    }

    
    
    @Test
    public void testFiltering() throws Exception {
        TestPublisher<String> publisher = new TestPublisher<>();
        TestSubscriber<String> subscriber = new TestSubscriber<>();
        
        Pipes.newPipe(publisher)
             .filter(num -> (Integer.parseInt(num) % 2) == 0)
             .consume(subscriber);
        
        publisher.push("1");
        publisher.push("2");
        publisher.push("3");
        publisher.completed();
        
              
        UnmodifiableIterator<String> elements = subscriber.getElements().iterator();
        Assert.assertEquals("2", elements.next());
        Assert.assertFalse(elements.hasNext());
    }
    

    
    
    @Test
    public void testMapping() throws Exception {
        TestPublisher<String> publisher = new TestPublisher<>();
        TestSubscriber<Integer> subscriber = new TestSubscriber<>();
        
        
        Pipes.newPipe(publisher)
             .map(element -> Integer.parseInt(element))
             .consume(subscriber);
        
        publisher.push("1");
        publisher.push("2");
        publisher.push("3");
        publisher.completed();
        
        
        UnmodifiableIterator<Integer> elements = subscriber.getElements().iterator();
        Assert.assertEquals((Integer) 1, elements.next());
        Assert.assertEquals((Integer) 2, elements.next());
        Assert.assertEquals((Integer) 3, elements.next());
        Assert.assertFalse(elements.hasNext());
    }
    
    
    
    @Test
    public void testMixed() throws Exception {
        TestPublisher<String> publisher = new TestPublisher<>();
        TestSubscriber<Integer> subscriber = new TestSubscriber<>();
        
        
        Pipes.newPipe(publisher).map(element -> Integer.parseInt(element))
             .filter(num -> (num % 2) == 0)
             .skip(1)
             .consume(subscriber);
        
        publisher.push("1");
        publisher.push("2");
        publisher.push("3");
        publisher.push("4");
        publisher.push("5");
        publisher.push("6");
        publisher.push("7");
        publisher.completed();
        
        
        UnmodifiableIterator<Integer> elements = subscriber.getElements().iterator();
        Assert.assertEquals((Integer) 4, elements.next());
        Assert.assertEquals((Integer) 6, elements.next());
        Assert.assertFalse(elements.hasNext());
    }

    
    
    @Test
    public void testMixedWithIterator() throws Exception {
        TestSubscriber<Integer> subscriber = new TestSubscriber<>();

        List<String> list = Lists.newArrayList();
        list.add("1");
        list.add("2");
        list.add("3");
        list.add("4");
        list.add("5");
        list.add("6");
        list.add("7");
        
        Pipes.newPipe(list.iterator()).map(element -> Integer.parseInt(element))
             .filter(num -> (num % 2) == 0)
             .skip(1)
             .consume(subscriber);
        
         
        UnmodifiableIterator<Integer> elements = subscriber.getElements().iterator();
        Assert.assertEquals((Integer) 4, elements.next());
        Assert.assertEquals((Integer) 6, elements.next());
        Assert.assertFalse(elements.hasNext());
    }

    
    
    @Test
    public void testMax() throws Exception {
        TestPublisher<String> publisher = new TestPublisher<>();
        TestSubscriber<Integer> subscriber = new TestSubscriber<>();
        
        
        Pipes.newPipe(publisher)
             .map(element -> Integer.parseInt(element))
             .filter(num -> (num % 2) == 0)
             .limit(2)
             .consume(subscriber);
        
        publisher.push("1");
        publisher.push("2");
        publisher.push("3");
        publisher.push("4");
        publisher.push("5");
        publisher.push("6");
        publisher.push("7");
        publisher.completed();
        
        
        UnmodifiableIterator<Integer> elements = subscriber.getElements().iterator();
        Assert.assertEquals((Integer) 2, elements.next());
        Assert.assertEquals((Integer) 4, elements.next());
        Assert.assertFalse(elements.hasNext());
    }
    

    
    
    
    @Test
    public void testBulk() throws Exception {
        TestPublisher<String> publisher = new TestPublisher<>();
        TestSubscriber<Integer> subscriber = new TestSubscriber<>();
        
        
        Pipes.newPipe(publisher)
             .map(element -> Integer.parseInt(element))
             .consume(subscriber);

        for (int i = 0; i < 100000; i++) {
            publisher.push(Integer.toString(i));
        }
        publisher.completed();
        
        
        ImmutableList<Integer> list = subscriber.getElements();
        Assert.assertEquals((Integer) 0, list.get(0));
        Assert.assertEquals((Integer) 96755, list.get(96755));
    }
    
    


    @Test
    public void testConsumerMappingAndFiltering() throws Exception {
        TestPublisher<String> publisher = new TestPublisher<>();
        TestConsumer<Integer> consumer = new TestConsumer<>();
        CompleteConsumer completeConsumer = new CompleteConsumer();
        
        
        Pipes.newPipe(publisher)
             .map(element -> Integer.parseInt(element))
             .filter(num -> (num % 2) == 0)
             .consume(consumer, null, completeConsumer);
        
        publisher.push("1");
        publisher.push("2");
        publisher.push("3");
        publisher.completed();
        
        
        completeConsumer.waitUntilCompleted();
        
        
        UnmodifiableIterator<Integer> elements = consumer.getElements().iterator();
        Assert.assertEquals((Integer) 2, elements.next());
        Assert.assertFalse(elements.hasNext());
        
    }
    
    
    
    @Test
    public void testErrorWithHandler() throws Exception {
        TestPublisher<String> publisher = new TestPublisher<>();
        TestErrornousConsumer<Integer> consumer = new TestErrornousConsumer<>();
        ErrorConsumer errorConsumer = new ErrorConsumer();
        CompleteConsumer completeConsumer = new CompleteConsumer();

        
        Pipes.newPipe(publisher)
             .map(element -> Integer.parseInt(element))
             .filter(num -> (num % 2) == 0)
             .consume(consumer, errorConsumer, completeConsumer);
        
        publisher.push("1");
        publisher.push("2");
        publisher.push("3");
        publisher.push("4");
        publisher.push("5");
        publisher.push("6");
        publisher.push("7");
        publisher.push("8");
        publisher.push("9");
        publisher.completed();
        
        
        completeConsumer.waitUntilCompleted();
        
        Assert.assertNotNull(errorConsumer.getError());
        
        UnmodifiableIterator<Integer> elements = consumer.getElements().iterator();
        Assert.assertEquals((Integer) 2, elements.next());
        Assert.assertEquals((Integer) 4, elements.next());
        Assert.assertFalse(elements.hasNext());
    }
    
    
    

    @Test
    public void testErrorWithoutHandler() throws Exception {
        TestPublisher<String> publisher = new TestPublisher<>();
        TestErrornousConsumer<Integer> consumer = new TestErrornousConsumer<>();
        CompleteConsumer completeConsumer = new CompleteConsumer();

        
        Pipes.newPipe(publisher)
             .map(element -> Integer.parseInt(element))
             .filter(num -> (num % 2) == 0)
             .consume(consumer, null, completeConsumer);
        
        publisher.push("1");
        publisher.push("2");
        publisher.push("3");
        publisher.push("4");
        publisher.push("5");
        publisher.push("6");
        publisher.push("7");
        publisher.push("8");
        publisher.push("9");
        publisher.completed();
        

        completeConsumer.waitUntilCompleted();
        
        UnmodifiableIterator<Integer> elements = consumer.getElements().iterator();
        Assert.assertEquals((Integer) 2, elements.next());
        Assert.assertEquals((Integer) 4, elements.next());
        Assert.assertFalse(elements.hasNext());
    }
    
    
    
    @Test
    public void testCompleratebleFuture() throws Exception {
        TestPublisher<String> publisher = new TestPublisher<>();
        TestSubscriber<String> subscriber = new TestSubscriber<>();
        
        
        Pipes.newPipe(publisher)
             .flatMap(text -> CompletableFuture.completedFuture(text))
             .consume(subscriber);
        
        publisher.push("1");
        publisher.push("2");
        publisher.push("3");
        publisher.completed();
        

        
        UnmodifiableIterator<String> elements = subscriber.getElements().iterator();
        Assert.assertEquals("1", elements.next());
        Assert.assertEquals("2", elements.next());
        Assert.assertEquals("3", elements.next());
        Assert.assertFalse(elements.hasNext());
    }

    
    
    
    
    private static final class TestConsumer<T> implements Consumer<T> {
        
        private final List<T> elements = Lists.newArrayList();
        
        @Override
        public void accept(T element) {
            elements.add(element);
        }
    
        public ImmutableList<T> getElements() {
            return ImmutableList.copyOf(elements);
        }
    }
    
    
    
    
    private static final class TestErrornousConsumer<T> implements Consumer<T> {
        
        private final List<T> elements = Lists.newArrayList();
        
        @Override
        public void accept(T element) {
            elements.add(element);
            if (elements.size() >= 2) {
                throw new RuntimeException("overflow");
            }
        }
    
        public ImmutableList<T> getElements() {
            return ImmutableList.copyOf(elements);
        }
    }
    
    
    
    private static final class ErrorConsumer implements Consumer<Throwable> {
        private final AtomicReference<Throwable> errorRef = new AtomicReference<>();
        
        @Override
        public void accept(Throwable t) {
            errorRef.set(t);
        }
        
        public Throwable getError() {
            return errorRef.get();
        }
    }
    
    

    private static final class CompleteConsumer implements Consumer<Void> {
        private boolean isCompleted = false;
        

        @Override
        public void accept(Void v) {
            synchronized (this) {
                isCompleted = true;
                notifyAll();
            }
        }
        
        
        public void waitUntilCompleted() {
            synchronized (this) {
                while (!isCompleted) {
                    try {
                        wait(500);
                    } catch (InterruptedException ignore) { }
                }
            }
        }

    }
    
    
    
    private static final class TestPublisher<T> implements Publisher<T> {
        private final AtomicReference<Optional<SimpleSubscription>> subscriptionRef = new AtomicReference<>(Optional.empty());
        private final AtomicBoolean moreDataExepected = new AtomicBoolean(true);
        
        public void push(T element) {
            subscriptionRef.get().ifPresent(subscription -> subscription.process(element));
        }
        
        public void completed() {
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
            
            private void teminateWithError(Throwable t) {
                if (isOpen.getAndSet(false)) {
                    subscriber.onError(t);
                }
            }
        }
    }
    
    
    
    private static class TestSubscriber<T> implements Subscriber<T> {
        private boolean isOpen = true;
        private final List<T> elements = Lists.newArrayList();
        private Subscription subscription;
        
        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }
        
        
        @Override
        public void onNext(T element) {
            synchronized (this) {
                elements.add(element);
            }
            subscription.request(1);
        }
        
        @Override
        public void onComplete() {
            synchronized (this) {
                isOpen = false;
                notifyAll();
            }
        }
        
        @Override
        public void onError(Throwable t) {
            synchronized (this) {
                isOpen = false;
                notifyAll();
            }
        }
        
        
        public ImmutableList<T> getElements() {
            
            synchronized (this) {
                while (isOpen) {
                    try {
                        wait(500);
                    } catch (InterruptedException ignore) { }
                }
            }
            
            synchronized (elements) {
                return ImmutableList.copyOf(elements);
            }
        }
    }
}