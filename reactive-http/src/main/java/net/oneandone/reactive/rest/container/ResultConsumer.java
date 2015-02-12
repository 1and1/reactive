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
package net.oneandone.reactive.rest.container;



import java.util.concurrent.CompletionException;
import java.util.function.BiConsumer;

import javax.ws.rs.container.AsyncResponse;




public class ResultConsumer implements BiConsumer<Object, Throwable> {
    
    private final AsyncResponse asyncResponse;
    
    private ResultConsumer(AsyncResponse asyncResponse) {
        this.asyncResponse = asyncResponse;
    }

    public static final BiConsumer<Object, Throwable> writeTo(AsyncResponse asyncResponse) {
        return new ResultConsumer(asyncResponse);
    }
    
    @Override
    public void accept(Object result, Throwable error) {
        
        if (error == null) {
            asyncResponse.resume(result);            
        } else {
            asyncResponse.resume(unwrapIfNecessary(error, 10));
        }
    }
    
    
    private static Throwable unwrapIfNecessary(Throwable ex, int maxDepth)  {

        if (isCompletionException(ex)) {
            Throwable e = ((CompletionException) ex).getCause();
            if (e != null) {
                if (maxDepth > 1) {
                    return unwrapIfNecessary(e, maxDepth - 1);
                } else {
                    return e;
                }
            }
        }
            
        return ex;
    }
    
    
    private static boolean isCompletionException(Throwable t) {
        return CompletionException.class.isAssignableFrom(t.getClass());
    }
}