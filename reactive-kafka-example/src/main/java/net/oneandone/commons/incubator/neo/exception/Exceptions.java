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
package net.oneandone.commons.incubator.neo.exception;


import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;



public class Exceptions {
    
    private Exceptions() { }
    

    
    
    
    /**
     * unwraps an exception 
     *  
     * @param ex         the exception to unwrap
     * @return the unwrapped exception
     */
    public static RuntimeException propagate(Throwable ex)  {
        Throwable t = unwrap(ex);
        return (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t);
    }
    
    
    /**
     * unwraps an exception 
     *  
     * @param ex         the exception to unwrap
     * @return the unwrapped exception
     */
    public static Throwable unwrap(Throwable ex)  {
        return unwrap(ex, 9);
    }
    
    
    /**
     * unwraps an exception 
     *  
     * @param ex         the exception to unwrap
     * @param maxDepth   the max depth
     * @return the unwrapped exception
     */
    private static Throwable unwrap(Throwable ex, int maxDepth)  {
        if (isCompletionException(ex) || isExecutionException(ex)) {
            Throwable e = ex.getCause();
            if (e != null) {
                if (maxDepth > 1) {
                    return unwrap(e, maxDepth - 1);
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
    
    
    private static boolean isExecutionException(Throwable t) {
        return ExecutionException.class.isAssignableFrom(t.getClass());
    }
}
