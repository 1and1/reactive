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


import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

import net.oneandone.reactive.utils.Utils;

import org.reactivestreams.Subscriber;

import com.google.common.collect.ImmutableList;




public interface ReactiveSink<T> extends Closeable {
    
    
    /**
     * @param element  the element to write
     * @return the write future  
     */
    CompletableFuture<Void> writeAsync(T element);

    
    /**
     * @param element the element to write
     */
    void write(T element);

    
       
    /**
     * shutdown the sink
     * 
     * @return the unprocessed element list
     */
    public ImmutableList<T> shutdownNow();
    
    
    /**
     * shutdown the queue. Let the unprocessed elements be processed
     */
    public void shutdown();
    
    
    @Override
    public void close();
    
    


    static <T> ReactiveSink<T> publish(Subscriber<T> subscriber) {
        return Utils.get(publishAsync(subscriber));
    }
    
    
    static <T> CompletableFuture<ReactiveSink<T>> publishAsync(Subscriber<T> subscriber) {
        return ReactiveSinkSubscription.newSubscriptionAsync(subscriber);
    }
}
