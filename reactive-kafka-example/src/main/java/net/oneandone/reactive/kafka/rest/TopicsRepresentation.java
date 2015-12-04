package net.oneandone.reactive.kafka.rest;



import java.util.List;

import javax.ws.rs.core.UriInfo;

import com.google.common.collect.ImmutableMap;

import net.oneandone.reactive.utils.LinksBuilder;





public class TopicsRepresentation {
    
    public ImmutableMap<String, Object> _links; 
    public List<TopicRepresentation> _elements;
    
    public TopicsRepresentation() {  }
    
    
    public TopicsRepresentation(UriInfo uriInfo, String path, List<TopicRepresentation> elements) { 
        this(LinksBuilder.create(uriInfo.getAbsolutePathBuilder().path(path).build()).build(), elements);
    }
    
    
    public TopicsRepresentation(ImmutableMap<String, Object> links, List<TopicRepresentation> elements) {
        this._links = links;
        this._elements = elements;
    }
}