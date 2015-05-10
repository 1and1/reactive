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


import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;



public class Reactives {
    
    private Reactives() { }
    
    public static <T, E extends RuntimeException> T get(Future<T> future, Function<Throwable, E> exceptionFactory) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw exceptionFactory.apply(e);
        } catch (ExecutionException e) {
            throw exceptionFactory.apply(e.getCause());
        }
    }
}
