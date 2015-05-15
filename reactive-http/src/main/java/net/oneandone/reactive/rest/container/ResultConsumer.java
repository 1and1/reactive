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



import java.util.function.BiConsumer;

import javax.ws.rs.container.AsyncResponse;

import net.oneandone.reactive.utils.Reactives;




/**
 * ResultConsumer
 *
 */
public class ResultConsumer implements BiConsumer<Object, Throwable> {
    
    private final AsyncResponse asyncResponse;
    
    private ResultConsumer(AsyncResponse asyncResponse) {
        this.asyncResponse = asyncResponse;
    }

    
    @Override
    public void accept(Object result, Throwable error) {
        if (error == null) {
            asyncResponse.resume(result);
        } else {
            asyncResponse.resume(Reactives.unwrap(error));
        }
    }
    
    
    /**
     * forwards the response to the REST response object. Includes error handling also 
     * @param asyncResponse the REST response
     * @return the BiConsumer consuming the response/error pair
     */
    public static final BiConsumer<Object, Throwable> writeTo(AsyncResponse asyncResponse) {
        return new ResultConsumer(asyncResponse);
    }
}