/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.reactive.utils.problem;




import javax.ws.rs.WebApplicationException;


import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import static com.google.common.base.Preconditions.*;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Optional;



public class Problem {
    private static final String TYPE = "__type__";
    private static final String STATUS = "__status__";
    private static final String DETAIL = "__detail__";
    
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
                                       .put(DETAIL, msg)
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
    
    
    public Optional<String> getDetail() {
        return Optional.ofNullable(problemData.get(DETAIL));
    }
    
    public Optional<String> getExceptionText() {
        return Optional.ofNullable(problemData.get("exception"));
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