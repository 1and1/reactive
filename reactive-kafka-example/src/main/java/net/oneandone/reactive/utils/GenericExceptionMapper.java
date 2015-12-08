package net.oneandone.reactive.utils;

import java.util.function.Function;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import com.google.common.collect.ImmutableMap;

import net.oneandone.reactive.utils.Problem;



public class GenericExceptionMapper implements ExceptionMapper<Throwable> {
    
    private final ImmutableMap<Class<?>, Function<Throwable, Problem>> problemMapperRegistry;
    
    public GenericExceptionMapper() {
        this(ImmutableMap.of());
    }
    
    private GenericExceptionMapper(ImmutableMap<Class<?>, Function<Throwable, Problem>> problemMapperRegistry) {
        this.problemMapperRegistry = problemMapperRegistry;
    }
   
    
    @SuppressWarnings("unchecked")
    public <T extends Throwable> GenericExceptionMapper withProblemMapper(Class<T> clazz, Function<T, Problem> mapper) {
        return new GenericExceptionMapper(ImmutableMap.<Class<?>, Function<Throwable, Problem>>builder()
                                                      .putAll(problemMapperRegistry)
                                                      .put(clazz, (Function<Throwable, Problem>) mapper)
                                                      .build()); 
    }
    
    
    @Override 
    public Response toResponse(Throwable ex) {
        Function<Throwable, Problem> problemMapper = problemMapperRegistry.get(ex.getClass());
        if (problemMapper == null) {
            problemMapper = e -> StdProblem.newServerErrorProblem();
        }
        
        return problemMapper.apply(ex)
                            .withDetail(ex.getMessage())
                            .withException(ex)
                            .toResponse();
    }
}