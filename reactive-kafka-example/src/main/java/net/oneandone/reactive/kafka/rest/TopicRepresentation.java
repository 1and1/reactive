package net.oneandone.reactive.kafka.rest;


import javax.ws.rs.core.UriInfo;

import com.google.common.collect.ImmutableMap;



public class TopicRepresentation {
    
    public ImmutableMap<String, Object> _links; 
    public String name;
    
    public TopicRepresentation() {  }
    
    
    public TopicRepresentation(UriInfo uriInfo, String path, String topicname) { 
        this(LinksBuilder.create(uriInfo.getAbsolutePathBuilder()
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
