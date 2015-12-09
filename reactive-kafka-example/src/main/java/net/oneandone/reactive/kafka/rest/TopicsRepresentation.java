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



import java.util.List;

import javax.ws.rs.core.UriInfo;

import com.google.common.collect.ImmutableMap;

import net.oneandone.reactive.utils.hypermedia.LinksBuilder;





public class TopicsRepresentation {
    
    public ImmutableMap<String, Object> _links; 
    public List<TopicRepresentation> _elements;
    
    public TopicsRepresentation() {  }
    
    
    public TopicsRepresentation(UriInfo uriInfo, String path, List<TopicRepresentation> elements) { 
        this(LinksBuilder.create(uriInfo.getBaseUriBuilder().path(path).build()).build(), elements);
    }
    
    
    public TopicsRepresentation(ImmutableMap<String, Object> links, List<TopicRepresentation> elements) {
        this._links = links;
        this._elements = elements;
    }
}