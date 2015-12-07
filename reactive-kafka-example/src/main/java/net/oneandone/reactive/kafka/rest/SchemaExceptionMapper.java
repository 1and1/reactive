package net.oneandone.reactive.kafka.rest;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import net.oneandone.avro.json.SchemaException;
import net.oneandone.reactive.utils.Problem;



public class SchemaExceptionMapper implements ExceptionMapper<SchemaException> {
    
    @Override 
    public Response toResponse(SchemaException ex) {
        return Problem.newMalformedRequestDataProblem()
                      .withDetail("unsupported scheme")
                      .withParam("schemaname", ex.getType())
                      .withException(ex)
                      .toResponse();
    }
}