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

import java.nio.ByteBuffer;



/**
 * DataHandler
 *     
 * @author grro
 */
interface StreamDataHandler {
 
    /**
     * error callback 
     * 
     * @param id the stream id 
     * @param error the cause
     */
    default void onError(String id, Throwable error) { };
    
    /**
     * content callback
     * @param id   the stream id 
     * @param data the received data
     */
    default void onContent(String id, ByteBuffer[] data) { }
}
    


    

