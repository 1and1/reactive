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
package net.oneandone.reactive.kafka.rest;


import javax.ws.rs.core.UriInfo;

import com.google.common.collect.ImmutableMap;

import net.oneandone.incubator.neo.hypermedia.LinksBuilder;



public class TopicRepresentation {
    
    public ImmutableMap<String, Object> _links; 
    public String name;
    
    public TopicRepresentation() {  }
    
    
    public TopicRepresentation(UriInfo uriInfo, String path, String topicname) { 
        this(LinksBuilder.create(uriInfo.getBaseUriBuilder()
                                        .path(path)
                                        .path(topicname)
                                        .build())
                         .withHref("events")
                         .withHref("schemas")
                         .build(),
             topicname);
    }
    
    
    public TopicRepresentation(ImmutableMap<String, Object> _links, String name) {
        this._links = _links;
        this.name = name;
    }
}
