package net.oneandone.reactive.kafka.rest;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import net.oneandone.reactive.utils.Problem;



public class DefaultExceptionMapper implements ExceptionMapper<Throwable> {
    
    @Override 
    public Response toResponse(Throwable ex) {
        return Problem.newServerErrorProblem()
                      .withException(ex)
                      .toResponse();
    }
}