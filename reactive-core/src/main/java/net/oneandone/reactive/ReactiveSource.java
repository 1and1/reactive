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



import java.util.concurrent.ExecutionException;

import org.reactivestreams.Publisher;

public interface ReactiveSource<T> extends Closeable {
    
    CompletableFuture<T> readAsync();

    
    T read();
    
    

    
    static <T> ReactiveSource<T> subscribe(Publisher<T> publisher) {
        try {
            return subscribeAsync(publisher).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }
    
    

    static <T> CompletableFuture<ReactiveSource<T>> subscribeAsync(Publisher<T> publisher) {
        CompletableFuture<ReactiveSource<T>> promise = new CompletableFuture<>();
        publisher.subscribe(new ReactiveSourceSubscriber<>(promise));
        
        return promise; 
    }
}
