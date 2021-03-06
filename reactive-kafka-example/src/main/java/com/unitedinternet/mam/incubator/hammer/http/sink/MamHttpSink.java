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

import net.oneandone.incubator.neo.http.sink.HttpSink;
import net.oneandone.incubator.neo.http.sink.HttpSinkBuilder;



public interface MamHttpSink  {
    
    
    static HttpSinkBuilder target(final String target) {
        return target(URI.create(target));
    }
    
    static HttpSinkBuilder target(final URI target) {
        Preconditions.checkNotNull(target);
        return HttpSink.target(target)
                       .withClient(new RestClientBuilder().build());
    }
}