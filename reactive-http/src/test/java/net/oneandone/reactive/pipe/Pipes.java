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
package net.oneandone.reactive.pipe;

import java.util.Iterator;

import org.reactivestreams.Publisher;







/**
 * Pipe factory
 *
 */
public class Pipes {

    /**
     * creates a new pipe instance 
     * @param publisher   the underlying publisher
     * @return a new pipe instance
     */
    public static <T> Pipe<T> newPipe(Publisher<T> publisher) {
        return new PublisherSourcedPipe<>(publisher);
    }
    
    /**
     * creates a new pipe instance
     * @param it  the underlying iterator
     * @return a new pipe instance
     */
    public static <T> Pipe<T> newPipe(Iterator<T> it) {
        return new PublisherSourcedPipe<>(new IteratorToPublisherAdapter<>(it));
    }
}