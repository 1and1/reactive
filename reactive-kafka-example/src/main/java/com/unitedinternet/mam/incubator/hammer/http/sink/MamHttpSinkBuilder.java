/*
 * Copyright 1&1 Internet AG, htt;ps://github.com/1and1/
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
package com.unitedinternet.mam.incubator.hammer.http.sink;

import java.net.URI;

import com.google.common.base.Preconditions;
import com.unitedinternet.mam.incubator.hammer.http.client.RestClientBuilder;

import net.oneandone.incubator.neo.http.sink.HttpSinkBuilder;


public interface MamHttpSinkBuilder extends HttpSinkBuilder  {
    
    /**
     * @param target the target uri
     * @return a new instance of the http sink
     */
    static HttpSinkBuilder create(final String target) {
        Preconditions.checkNotNull(target);
        return create(URI.create(target));
    }

    
    /**
     * @param target the target uri
     * @return a new instance of the http sink
     */
    static HttpSinkBuilder create(final URI target) {
        Preconditions.checkNotNull(target);
        return HttpSinkBuilder.create(target)
                              .withClient(new RestClientBuilder().build());
    }
}