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
package net.oneandone.reactive;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import net.oneandone.reactive.utils.SubscriberNotifier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;






public class ReactiveSink<T> implements Consumer<T> {
    private final SinkSubscription sinkSubscription;
    
    private ReactiveSink(Subscriber<? super T> subscriber, int buffersize) {
        sinkSubscription = new SinkSubscription(subscriber, buffersize);
        sinkSubscription.init();
        
    }
   
   
    @Override
    public void accept(T t) {
        sinkSubscription.onNext(t);
    }
    
    
    /*
    
    public ImmutableList<T> shutdownNow() {
        sinkSubscription.onComplete();
        
    }
    */
    
    public void shutdown() {
        
    }
   

    public ImmutableList<T> getBuffered() {
        return sinkSubscription.getBuffered();
    }
    
    
    private final class SinkSubscription implements Subscription {
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        private final SubscriberNotifier<T> subscriberNotifier;
        
        private final Object consumeLock = new Object();
        private final BlockingQueue<T> queue;
        private long pendingRequests = 0;
        

        public SinkSubscription(Subscriber<? super T> subscriber, int buffersize) {
            subscriberNotifier = new SubscriberNotifier<>(subscriber, this);
            queue = Queues.newLinkedBlockingQueue(buffersize);
        }

        public void init() {
            subscriberNotifier.start();
        }

        
        public void onNext(T t) {
            if (!isOpen.get()) {
                throw new IllegalStateException("stream is already closed");
            }
            
            synchronized (consumeLock) {
                queue.add(t);
                process();
            }
        }

        public void onComplete() {
            subscriberNotifier.notifyOnComplete();
        }

        @Override
        public void cancel() {
           // close();
        }

        @Override
        public void request(long n) {
            synchronized (consumeLock) {
                pendingRequests =+ n;
                process();
            }
        }
        
        
        public ImmutableList<T> getBuffered() {
            synchronized (consumeLock) {
                return ImmutableList.copyOf((T[]) queue.toArray());
            }
        }
        
        
        private void process() {
            while (pendingRequests > 0) {
                T t = queue.poll();
                if (t == null) {
                    return;
                } else {
                    pendingRequests--; 
                    subscriberNotifier.notifyOnNext(t);
                }
            }
        }
    }
    

  
    public static <T> ReactiveSinkBuilder buffer(int buffersize) {
        return new ReactiveSinkBuilder(buffersize);
    }
    
    
    public static final class ReactiveSinkBuilder {
        private final int buffersize;
    
        ReactiveSinkBuilder(int buffersize) {
            this.buffersize = buffersize;
        }
        
        
        public <T> ReactiveSink<T> subscribe(Subscriber<? super T> subscriber) {
            return new ReactiveSink<T>(subscriber, buffersize);
        }
    }
}
