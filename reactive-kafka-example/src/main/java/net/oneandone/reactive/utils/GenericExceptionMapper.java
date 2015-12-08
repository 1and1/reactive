package net.oneandone.reactive.utils;

import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import net.oneandone.reactive.utils.Problem;



public class GenericExceptionMapper implements ExceptionMapper<Throwable> {
    private static final String ALL = "*"; 
    private final ImmutableMap<Class<?>, ImmutableMap<String, Function<Throwable, Problem>>> problemMapperRegistry;
    
    private @Context HttpServletRequest httpReq;
    
    
    public GenericExceptionMapper() {
        this(ImmutableMap.of());
    }
    
    private GenericExceptionMapper(ImmutableMap<Class<?>, ImmutableMap<String, Function<Throwable, Problem>>> problemMapperRegistry) {
        this.problemMapperRegistry = problemMapperRegistry;
    }
   
    public <T extends Throwable> GenericExceptionMapper withProblemMapper(Class<T> clazz, Function<T, Problem> mapper) {
        return withProblemMapper(clazz, ALL, mapper); 
    }
    
    public <T extends Throwable> GenericExceptionMapper withProblemMapper(Class<T> clazz, String method, Function<T, Problem> mapper) {
        return withProblemMapper(clazz, ImmutableList.of(method), mapper);
    }

    public <T extends Throwable> GenericExceptionMapper withProblemMapper(Class<T> clazz, String method1, String method2, Function<T, Problem> mapper) {
        return withProblemMapper(clazz, ImmutableList.of(method1, method2), mapper);
    }

    
    @SuppressWarnings("unchecked")
    private <T extends Throwable> GenericExceptionMapper withProblemMapper(Class<T> clazz, ImmutableList<String> methods, Function<T, Problem> mapper) {

        Map<String, Function<Throwable, Problem>> mappers = (problemMapperRegistry.get(clazz) == null) ? Maps.newHashMap() 
                                                                                                       : Maps.newHashMap(problemMapperRegistry.get(clazz));
        methods.forEach(method -> mappers.put(method.toUpperCase(Locale.US), (Function<Throwable, Problem>) mapper));
        
        
        Map<Class<?>, ImmutableMap<String, Function<Throwable, Problem>>> newProblemMapperRegistry = Maps.newHashMap(problemMapperRegistry);
        newProblemMapperRegistry.put(clazz, ImmutableMap.copyOf(mappers));
        
        
        return new GenericExceptionMapper(ImmutableMap.copyOf(newProblemMapperRegistry)); 

    }

    
    @Override 
    public Response toResponse(Throwable ex) {
        
        // default handler 
        Function<Throwable, Problem> problemMapper = e -> StdProblem.newServerErrorProblem();
        
        // try to get a more specific one
        ImmutableMap<String, Function<Throwable, Problem>> problemMappers = problemMapperRegistry.get(ex.getClass());
        if (problemMappers != null) {
            if (problemMappers.containsKey(httpReq.getMethod().toUpperCase(Locale.US))) {
                problemMapper = problemMappers.get(httpReq.getMethod().toUpperCase(Locale.US));
            } else if (problemMappers.containsKey(ALL)) {
                problemMapper = problemMappers.get(ALL);
            }
        }

        return problemMapper.apply(ex)
                            .withDetail(ex.getMessage())
                            .withException(ex)
                            .toResponse();
    }
}