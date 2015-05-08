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
package net.oneandone.reactive.sse.client;


import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;




class Utils {
    private Utils() { }
    
    public static <E> ErrorFunction<E> forwardTo(CompletableFuture<?> promise) {
        return new ErrorFunction<>(promise);
    }

    private static class ErrorFunction<T> implements Function<Throwable, T> {
            
        private final CompletableFuture<?> promise;
        
        
        private ErrorFunction(CompletableFuture<?> promise) {
            this.promise = promise;
        }
        
        @Override
        public T apply(Throwable error) {
            promise.completeExceptionally(error);
            return null;
        }
    }
    
    
    
    public static <E> Handler<E> then(Consumer<Throwable> consumer) {
        return new Handler<>(consumer);
    }
    

    private static class Handler<T> implements Function<Throwable, T> {
            
        private final Consumer<Throwable> consumer;
        
        private Handler(Consumer<Throwable> consumer) {
            this.consumer = consumer;
        }
        
        @Override
        public T apply(Throwable error) {
            consumer.accept(error);
            return null;
        }
    }
}

