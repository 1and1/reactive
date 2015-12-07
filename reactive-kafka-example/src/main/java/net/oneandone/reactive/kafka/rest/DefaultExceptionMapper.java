package net.oneandone.reactive.kafka.rest;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;



public class DefaultExceptionMapper implements ExceptionMapper<Throwable> {
    
    @Override 
    public Response toResponse(Throwable ex) {
 
        return Response.status(500)
                       .entity(ex.toString())
                       .type(MediaType.TEXT_PLAIN)
                       .build();   
    }
}