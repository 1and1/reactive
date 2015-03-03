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



import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Subscriber;


/**
 * Pipe
 *
 * @param <T> the element type
 */
public interface Pipe<T> {
    
    
    /**
     * maps the elements 
     * @param fn  the mapping function
     * @param <V> the new element type future
     * @return the mapped flow
     */
    <V> Pipe<V> flatMap(Function<? super T, CompletableFuture<? extends V>> fn);
          
    
    /**
     * maps the elements
     * @param fn  the mapping function
     * @param <V> the new element type 
     * @return the mapped flow
     */
    <V> Pipe<V> map(Function<? super T, ? extends V> fn);
    
    
    /**
     * @param predicate a non-interfering, stateless predicate to apply to each element to determine if it should be included
     * @return a stream consisting of the elements of this stream that match the given predicate
     */
    Pipe<T> filter(Predicate<? super T> predicate);
    
    
    /**
     * @param maxSize   the number of elements the stream should be limited to
     * @return a stream consisting of the elements of this stream, truncated to be no longer than maxSize in length
     */
    Pipe<T> limit(long maxSize);
    
    
    /**
     * @param n  the number of leading elements to skip
     * @return a stream consisting of the remaining elements of this stream after discarding the first n elements of the stream
     */
    Pipe<T> skip(long n);
    
    
    /**
     * consumes the flow
     * @param subscriber  the subscriber which consumes the flow
     */
    void consume(Subscriber<? super T> subscriber);
    
    
    /**
     * consumes the flow
     * @param consumer   the consumer of the flow
     */
    void consume(Consumer<? super T> consumer);
    
    
    /**
     * consumes the flow
     * @param consumer       the consumer of the flow
     * @param errorConsumer  the error consumer
     */
    void consume(Consumer<? super T> consumer, Consumer<? super Throwable> errorConsumer);
    
    
    /**
     * consumes the flow
     * @param consumer         the consumer of the flow
     * @param errorConsumer    the error consumer
     * @param completeConsumer the complete consumer
     */
    void consume(Consumer<? super T> consumer, Consumer<? super Throwable> errorConsumer, Consumer<Void> completeConsumer);
}
    