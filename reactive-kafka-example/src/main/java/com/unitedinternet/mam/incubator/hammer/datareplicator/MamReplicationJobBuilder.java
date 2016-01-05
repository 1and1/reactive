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
package com.unitedinternet.mam.incubator.hammer.datareplicator;

import java.net.URI;

import com.google.common.base.Preconditions;
import com.unitedinternet.mam.incubator.hammer.http.client.RestClientBuilder;

import net.oneandone.incubator.neo.datareplicator.ReplicationJobBuilder;



public interface MamReplicationJobBuilder extends ReplicationJobBuilder {
    
    /**
     * @param uri  the source uri. Supported schemes are <i>file</i>, <i>http</i>, <i>https</i> and <i>classpath</i> 
     *             (e.g. file:/C:/dev/workspace/reactive2/reactive-kafka-example/src/main/resources/schemas.zip, 
     *              classpath:schemas/schemas.zip, http://myserver/schemas.zip)  
     */
    static ReplicationJobBuilder create(final String uri) {
        Preconditions.checkNotNull(uri);
        return create(URI.create(uri));
    }
    
    /**
     * @param uri  the source uri. Supported schemes are <i>file</i>, <i>http</i>, <i>https</i> and <i>classpath</i> 
     *             (e.g. file:/C:/dev/workspace/reactive2/reactive-kafka-example/src/main/resources/schemas.zip, 
     *              classpath:schemas/schemas.zip, http://myserver/schemas.zip)  
     */
    static ReplicationJobBuilder create(final URI uri) {
        Preconditions.checkNotNull(uri);
        return ReplicationJobBuilder.create(uri)
                                    .withClient(new RestClientBuilder().build()); 
    }
    
}