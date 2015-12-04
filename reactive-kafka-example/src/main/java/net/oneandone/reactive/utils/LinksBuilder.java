package net.oneandone.reactive.utils;



import java.net.URI;
import java.util.Locale;

import javax.ws.rs.core.UriInfo;

import com.google.common.collect.ImmutableMap;





public class LinksBuilder {
        
    private final ImmutableMap<String, Object> links;
    private final URI selfHref;
    
    private LinksBuilder(URI selfHref, ImmutableMap<String, Object> links) {
        this.selfHref = selfHref;
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
            return withHref(name, URI.create(selfHref.toString() + "/" + href));
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
