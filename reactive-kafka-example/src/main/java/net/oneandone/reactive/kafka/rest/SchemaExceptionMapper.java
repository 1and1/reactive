package net.oneandone.reactive.kafka.rest;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import net.oneandone.avro.json.SchemaException;
import net.oneandone.reactive.utils.ProblemBuilder;



public class SchemaExceptionMapper implements ExceptionMapper<SchemaException> {
    
    @Override 
    public Response toResponse(SchemaException ex) {
        return ProblemBuilder.problem(400, "urn:problem:net.oneandone.reactive.kafka.rest:schema-error")
                             .param("schemaname", ex.getType())
                             .buildResponse();
    }
}