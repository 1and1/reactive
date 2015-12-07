package net.oneandone.reactive.utils;



import java.util.Map;

import javax.ws.rs.core.Response;

import com.google.common.collect.Maps;



public class ProblemBuilder {
    private final int status;
    private final Map<String, Object> attributes = Maps.newLinkedHashMap();

    private ProblemBuilder(int status, String urn) {
        this.status = status;
        this.attributes.put("status", status);
        this.attributes.put("type", urn);
    }


    public static ProblemBuilder problem(int status, String urn) {
        return new ProblemBuilder(status, urn);
    }


    public ProblemBuilder param(String name, String value) {
        this.attributes.put(name, value);
        return this;
    }


    public Response buildResponse() {
        return Response.status(status)
                       .entity(attributes)
                       .type("application/problem+json").build();
    }
}