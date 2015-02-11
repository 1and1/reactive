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
package net.oneandone.reactive.flow;

import java.util.Iterator;

import org.reactivestreams.Publisher;







/**
 * Flow factory
 *
 */
public class Flows {

    /**
     * creates a new flow instance 
     * @param publisher   the underlying publisher
     * @return a new flow instance
     */
    public static <T> Flow<T> newFlow(Publisher<T> publisher) {
        return new PublisherSourcedFlow<>(publisher);
    }
    
    /**
     * creates a new flow instance
     * @param it  the underlying iterator
     * @return a new flow instance
     */
    public static <T> Flow<T> newFlow(Iterator<T> it) {
        return new PublisherSourcedFlow<>(new SupplierToPublisherAdapter<>(it));
    }
}