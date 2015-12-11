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
package net.oneandone.reactive.utils.hypermedia;



import java.net.URI;
import java.util.Locale;

import javax.ws.rs.core.UriInfo;

import com.google.common.collect.ImmutableMap;





public class LinksBuilder {
        
    private final ImmutableMap<String, Object> links;
    private final URI selfHref;
    
    private LinksBuilder(URI selfHref, ImmutableMap<String, Object> links) {
        this.selfHref = selfHref.toString().endsWith("/") ? selfHref : URI.create(selfHref.toString() + "/");
        this.links = links;
    }

    public static LinksBuilder create(UriInfo uriInfo) {
        return create(uriInfo.getAbsolutePathBuilder().build());
    }
    
    public static LinksBuilder create(URI selfHref) {
        return new LinksBuilder(selfHref, ImmutableMap.of()).withHref("self", selfHref);
    }
    
    public LinksBuilder withHref(String name) {
        return withHref(name, name);
    }
    
    public LinksBuilder withHref(String name, String href) {
        if (name.toLowerCase(Locale.US).startsWith("http")) {
            return withHref(name, URI.create(href));
        } else {
            return withHref(name, URI.create(selfHref.toString() + href));
        }
    }
    
    public LinksBuilder withHref(String name, URI href) {
        return new LinksBuilder(selfHref,
                                ImmutableMap.<String, Object>builder()
                                            .putAll(links)
                                            .put(name, ImmutableMap.of("href", href.toString()))
                                            .build());
    }

   
    public ImmutableMap<String, Object> build() {
        return links;
    }
}
