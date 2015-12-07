package net.oneandone.reactive.utils;




import javax.ws.rs.WebApplicationException;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;



public final class Problem {
    private final int status;
    private final ImmutableMap<String, String> problemData;
    
    private Problem(int status, ImmutableMap<String, String> problemData) {
        this.status = status;
        this.problemData = problemData;
    }

    public Problem withDetail(String msg) {
        return new Problem(status, ImmutableMap.<String, String>builder()
                                               .putAll(problemData)
                                               .put("detail", msg)
                                               .build());
    }
        
    public Problem withParam(String name, String value) {
        return new Problem(status, ImmutableMap.<String, String>builder()
                                               .putAll(problemData)
                                               .put(name, value)
                                               .build());
    }
    
    public Problem withException(Throwable t) {
        return new Problem(status, ImmutableMap.<String, String>builder()
                                               .putAll(problemData)
                                               .put("exceptionClass", t.getClass().getSimpleName())
                                               .put("exception", t.toString())
                                               .put("exceptionStackTrace", Throwables.getStackTraceAsString(t).replace("\t", "   "))
                                               .build());
    }
    
    
    public Response toResponse() {
        return Response.status(status)
                       .entity(problemData)
                       .type("application/problem+json").build();
    }

    
    public boolean is(int status, String problemtype) {
        return (this.status == status) && problemtype.equals(this.problemData.get("type"));
    }

    
    public static Problem newProblem(int status, String urn) {
        return new Problem(status, ImmutableMap.of("status", Integer.toString(status), "type", urn));
    }



    public boolean isMalformedRequestDataProblem() {
        return is(400, "urn:problem:formed-request-data");
    }

    public static Problem newMalformedRequestDataProblem() {
        return newProblem(400, "urn:problem:formed-request-data");
    }

    
    public boolean isServerErrorProblem() {
        return is(500, "urn:problem:server-error");
    }

    public static Problem newServerErrorProblem() {
        return newProblem(500, "urn:problem:server-error");
    }


    
    public static Problem of(WebApplicationException wae) {
        
        Response response = wae.getResponse();
        try {
            response.bufferEntity();
            Map<String, String> problemData = response.readEntity(new GenericType<Map<String, String>>() { });
            if (problemData == null) {
                problemData = Maps.newHashMap();
            }
            
            return new Problem(wae.getResponse().getStatus(), ImmutableMap.copyOf(problemData));
        } finally {
            response.close();
        }
    }

}