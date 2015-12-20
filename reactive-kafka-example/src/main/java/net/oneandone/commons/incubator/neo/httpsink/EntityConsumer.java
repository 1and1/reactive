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
package net.oneandone.commons.incubator.neo.httpsink;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;


public interface EntityConsumer extends BiConsumer<Object, String>, Closeable {

    @Override
    default void accept(Object entity, String mediaType) {
        try {
            acceptAsync(entity, mediaType).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t; 
            } else {
                throw new RuntimeException(t);
            }
        }
    }

    CompletableFuture<Boolean> acceptAsync(Object entity, String mediaType);
    
    int getQueueSize();
    
    long getNumSuccess();
    
    long getNumRetries();
    
    long getNumDiscarded();

    @Override
    void close();
}
