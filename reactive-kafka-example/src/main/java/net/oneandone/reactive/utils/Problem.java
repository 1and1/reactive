package net.oneandone.reactive.utils;




import javax.ws.rs.WebApplicationException;


import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;



public class Problem {
    private static final String TYPE = "type";
    private static final String STATUS = "status";
    
    private final ImmutableMap<String, String> problemData;
    
    protected Problem(ImmutableMap<String, String> problemData) {
        this.problemData = problemData;
    }
    
    protected ImmutableMap<String, String> getProblemdata() {
        return problemData;
    }
    
    public Problem withDetail(String msg) {
        return new Problem(ImmutableMap.<String, String>builder()
                                       .putAll(problemData)
                                       .put("detail", msg)
                                       .build());
    }
        
    public Problem withParam(String name, String value) {
        if (value == null) {
            return this;
        } else {
            return new Problem(ImmutableMap.<String, String>builder()
                                           .putAll(problemData)
                                           .put(name, value)
                                           .build());
        }
    }
    
    public Problem withException(Throwable t) {
        return new Problem(ImmutableMap.<String, String>builder()
                                       .putAll(problemData)
                                       .put("exceptionClass", t.getClass().getSimpleName())
                                       .put("exception", t.toString())
                                       .put("exceptionStackTrace", Throwables.getStackTraceAsString(t).replace("\t", "   "))
                                       .build());
    }
    
    
    @Override
    public String toString() {
        return Joiner.on("\r\n").withKeyValueSeparator("=").join(problemData);
    }
    
    
    public Response toResponse() {
        return Response.status(Integer.parseInt(problemData.get(STATUS)))
                       .entity(problemData)
                       .type("application/problem+json").build();
    }

    
    public boolean is(int status, String problemtype) {
        return (status == Integer.parseInt(this.problemData.get(STATUS))) && 
                problemtype.equals(this.problemData.get(TYPE));
    }

    
    public static Problem newProblem(int status, String urn) {
        return new Problem(newProblemdata(status, urn));
    }

    protected static ImmutableMap<String, String> newProblemdata(int status, String urn) {
        return ImmutableMap.of(STATUS, Integer.toString(checkNotNull(status)), 
                               TYPE, checkNotNull(urn));
    }

    
    public static Problem of(WebApplicationException wae) {
        return new Problem(parseProblemData(wae));
    }
        
    protected static ImmutableMap<String, String> parseProblemData(WebApplicationException wae) {
        Response response = wae.getResponse();
        try {
            response.bufferEntity();
            Map<String, String> problemData = response.readEntity(new GenericType<Map<String, String>>() { });
            if (problemData == null) {
                problemData = Maps.newHashMap();
            }
            
            return ImmutableMap.copyOf(problemData);
        } finally {
            response.close();
        }
    }
}