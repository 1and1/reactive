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
package net.oneandone.reactive.rest;



import java.util.concurrent.CompletableFuture;



public class Dao {

    
    CompletableFuture<String> readAsync(long id) {
        CompletableFuture<String> future = new CompletableFuture<>();
        
        new Thread() {
            public void run() {
                try {
                    Thread.sleep(120);
                } catch (InterruptedException ignore)  { }
                
                if (id == 666) {
                    future.completeExceptionally(new IllegalStateException());
                } else {
                    future.complete(Long.toString(id));
                }
                
                future.complete(Long.toString(id));
            };
        }.start();
        
        return future;
    }
    
    
    
    CompletableFuture<Void> deleteAsync(long id) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        new Thread() {
            public void run() {
                try {
                    Thread.sleep(120);
                } catch (InterruptedException ignore)  { }
                
                future.complete(null);
            };
        }.start();
        
        return future;
    }
    
}