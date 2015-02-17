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
package net.oneandone.reactive.rest.client;



import java.lang.annotation.Annotation;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotAcceptableException;
import javax.ws.rs.NotAllowedException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.RedirectionException;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.AsyncInvoker;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;





/**
 * Completable JAX-RS Client
 */
public class RxClient implements Client {
    private final Client client;

    /**
     * @param client thr underlying client
     */
    public RxClient(Client client) {
        this.client = client;
    }
    
    @Override
    public void close() {
        client.close();
    }

    @Override
    public RxWebTarget target(String uri) {
        return new RxWebTarget(client.target(uri));
    }

    @Override
    public RxWebTarget target(URI uri) {
        return new RxWebTarget(client.target(uri));
    }

    @Override
    public RxWebTarget target(UriBuilder uriBuilder) {
        return new RxWebTarget(client.target(uriBuilder));
    }

    @Override
    public RxWebTarget target(Link link) {
        return new RxWebTarget(client.target(link));
    }

    @Override
    public RxBuilder invocation(Link link) {
        return new RxBuilder(client.invocation(link));
    }

    @Override
    public SSLContext getSslContext() {
        return client.getSslContext();
    }

    @Override
    public HostnameVerifier getHostnameVerifier() {
        return client.getHostnameVerifier();
    }

    @Override
    public Configuration getConfiguration() {
        return client.getConfiguration();
    }

    @Override
    public RxClient property(String name, Object value) {
        return new RxClient(client.property(name, value));
    }

    @Override
    public RxClient register(Class<?> componentClass) {
        return new RxClient(client.register(componentClass));
    }

    @Override
    public RxClient register(Class<?> componentClass, int priority) {
        return new RxClient(client.register(componentClass, priority));
    }

    @Override
    public RxClient register(Class<?> componentClass, Class<?>... contracts) {
        return new RxClient(client.register(componentClass, contracts));
    }

    @Override
    public RxClient register(Class<?> componentClass, Map<Class<?>, Integer> contracts) {
        return new RxClient(client.register(componentClass, contracts));
    }

    @Override
    public RxClient register(Object component) {
        return new RxClient(client.register(component));
    }

    @Override
    public RxClient register(Object component, int priority) {
        return new RxClient(client.register(component, priority));
    }

    @Override
    public RxClient register(Object component, Class<?>... contracts) {
        return new RxClient(client.register(component, contracts));
    }

    @Override
    public RxClient register(Object component, Map<Class<?>, Integer> contracts) {
        return new RxClient(client.register(component, contracts));
    }
    
   

    
   
    /**
     * Completable JAX-RS WebTarget
     */
    public static class RxWebTarget implements WebTarget {
        private final WebTarget webTarget;
      
        private RxWebTarget(WebTarget webTarget) {
            this.webTarget = webTarget;
        }
        
        @Override    
        public URI getUri() {
            return webTarget.getUri();
        }

        @Override
        public UriBuilder getUriBuilder() {
            return webTarget.getUriBuilder();
        }

        @Override
        public RxWebTarget path(String path) {
            return new RxWebTarget(webTarget.path(path));
        }

        @Override
        public RxWebTarget resolveTemplate(String name, Object value) {
            return new RxWebTarget(webTarget.resolveTemplate(name, value));
        }

        @Override
        public RxWebTarget resolveTemplate(String name, Object value, boolean encodeSlashInPath) {
            return new RxWebTarget(webTarget.resolveTemplate(name, value, encodeSlashInPath));
        }

        @Override
        public RxWebTarget resolveTemplateFromEncoded(String name, Object value) {
            return new RxWebTarget(webTarget.resolveTemplateFromEncoded(name, value));
        }

        @Override
        public RxWebTarget resolveTemplates(Map<String, Object> templateValues) {
            return new RxWebTarget(webTarget.resolveTemplates(templateValues));
        }

        @Override
        public Configuration getConfiguration() {
            return webTarget.getConfiguration();
        }

        @Override
        public RxWebTarget resolveTemplates(Map<String, Object> templateValues, boolean encodeSlashInPath) {
            return new RxWebTarget(webTarget.resolveTemplates(templateValues, encodeSlashInPath));
        }

        @Override
        public RxWebTarget property(String name, Object value) {
            return new RxWebTarget(webTarget.property(name, value));
        }

        @Override
        public RxWebTarget register(Class<?> componentClass) {
            return new RxWebTarget(webTarget.register(componentClass));
        }

        @Override
        public RxWebTarget resolveTemplatesFromEncoded(Map<String, Object> templateValues) {
            return new RxWebTarget(webTarget.resolveTemplatesFromEncoded(templateValues));
        }

        @Override
        public RxWebTarget register(Class<?> componentClass, int priority) {
            return new RxWebTarget(webTarget.register(componentClass, priority));
        }

        @Override
        public RxWebTarget matrixParam(String name, Object... values) {
            return new RxWebTarget(webTarget.matrixParam(name, values));
        }

        @Override
        public RxWebTarget register(Class<?> componentClass, Class<?>... contracts) {
            return new RxWebTarget(webTarget.register(componentClass, contracts));
        }

        @Override
        public RxWebTarget queryParam(String name, Object... values) {
            return new RxWebTarget(webTarget.queryParam(name, values));
        }

        @Override
        public RxWebTarget register(Class<?> componentClass, Map<Class<?>, Integer> contracts) {
            return new RxWebTarget(webTarget.register(componentClass, contracts));
        }

        @Override
        public RxBuilder request() {
            return new RxBuilder(webTarget.request());
        }

        @Override
        public RxBuilder request(String... acceptedResponseTypes) {
            return new RxBuilder(webTarget.request(acceptedResponseTypes));
        }

        @Override
        public RxBuilder request(MediaType... acceptedResponseTypes) {
            return new RxBuilder(webTarget.request(acceptedResponseTypes));
        }

        @Override
        public RxWebTarget register(Object component) {
            return new RxWebTarget(webTarget.register(component));
        }

        @Override
        public RxWebTarget register(Object component, int priority) {
            return new RxWebTarget(webTarget.register(component, priority));
        }

        @Override
        public RxWebTarget register(Object component, Class<?>... contracts) {
            return new RxWebTarget(webTarget.register(component, contracts));
        }

        @Override
        public RxWebTarget register(Object component, Map<Class<?>, Integer> contracts) {
            return new RxWebTarget(webTarget.register(component, contracts));
        }
    }

    
    /**
     * Completable JAX-RS Builder
     */
    public static class RxBuilder implements Builder {
        private final Builder builder;
        
        private RxBuilder(Builder builder) {
            this.builder = builder;
        }        
       
        @Override
        public Response get() {
            return builder.get();
        }

        @Override
        public <T> T get(Class<T> responseType) {
            return builder.get(responseType);
        }

        @Override
        public <T> T get(GenericType<T> responseType) {
            return builder.get(responseType);
        }

        @Override
        public Response put(Entity<?> entity) {
            return builder.put(entity);
        }

        @Override
        public Invocation build(String method) {
            return builder.build(method);
        }

        @Override
        public Invocation build(String method, Entity<?> entity) {
            return builder.build(method, entity);
        }

        @Override
        public <T> T put(Entity<?> entity, Class<T> responseType) {
            return builder.put(entity, responseType);
        }

        @Override
        public Invocation buildGet() {
            return builder.buildGet();
        }

        @Override
        public Invocation buildDelete() {
            return builder.buildDelete();
        }

        @Override
        public Invocation buildPost(Entity<?> entity) {
            return builder.buildPost(entity);
        }

        @Override
        public Invocation buildPut(Entity<?> entity) {
            return builder.buildPut(entity);
        }

        @Override
        public <T> T put(Entity<?> entity, GenericType<T> responseType) {
            return builder.put(entity, responseType);
        }

        @Override
        public AsyncInvoker async() {
            return new RxInvoker(builder.async());
        }

        public RxInvoker rx() {
            return new RxInvoker(builder.async());
        }

        
        @Override
        public RxBuilder accept(String... mediaTypes) {
            return new RxBuilder(builder.accept(mediaTypes));
        }

        @Override
        public RxBuilder accept(MediaType... mediaTypes) {
            return new RxBuilder(builder.accept(mediaTypes));
        }

        @Override
        public RxBuilder acceptLanguage(Locale... locales) {
            return new RxBuilder(builder.acceptLanguage(locales));
        }

        @Override
        public RxBuilder acceptLanguage(String... locales) {
            return new RxBuilder(builder.acceptLanguage(locales));
        }

        @Override
        public RxBuilder acceptEncoding(String... encodings) {
            return new RxBuilder(builder.acceptEncoding(encodings));
        }

        @Override
        public Response post(Entity<?> entity) {
            return builder.post(entity);
        }

        @Override
        public RxBuilder cookie(Cookie cookie) {
            return new RxBuilder(builder.cookie(cookie));
        }

        @Override
        public RxBuilder cookie(String name, String value) {
            return new RxBuilder(builder.cookie(name, value));
        }

        @Override
        public RxBuilder cacheControl(CacheControl cacheControl) {
            return new RxBuilder(builder.cacheControl(cacheControl));
        }

        @Override
        public RxBuilder header(String name, Object value) {
            return new RxBuilder(builder.header(name, value));
        }

        @Override
        public <T> T post(Entity<?> entity, Class<T> responseType) {
            return builder.post(entity, responseType);
        }

        @Override
        public RxBuilder headers(MultivaluedMap<String, Object> headers) {
            return new RxBuilder(builder.headers(headers));
        }

        @Override
        public RxBuilder property(String name, Object value) {
            return new RxBuilder(builder.property(name, value));
        }

        @Override
        public <T> T post(Entity<?> entity, GenericType<T> responseType) {
            return builder.post(entity, responseType);
        }

        @Override
        public Response delete() {
            return builder.delete();
        }

        @Override
        public <T> T delete(Class<T> responseType) {
            return builder.delete(responseType);
        }

        @Override
        public <T> T delete(GenericType<T> responseType) {
            return builder.delete(responseType);
        }

        @Override
        public Response head() {
            return builder.head();
        }

        @Override
        public Response options() {
            return builder.options();
        }

        @Override
        public <T> T options(Class<T> responseType) {
            return builder.options(responseType);
        }

        @Override
        public <T> T options(GenericType<T> responseType) {
            return builder.options(responseType);
        }

        @Override
        public Response trace() {
            return builder.trace();
        }

        @Override
        public <T> T trace(Class<T> responseType) {
            return builder.trace(responseType);
        }

        @Override
        public <T> T trace(GenericType<T> responseType) {
            return builder.trace(responseType);
        }

        @Override
        public Response method(String name) {
            return builder.method(name);
        }

        @Override
        public <T> T method(String name, Class<T> responseType) {
            return builder.method(name, responseType);
        }

        @Override
        public <T> T method(String name, GenericType<T> responseType) {
            return builder.method(name, responseType);
        }

        @Override
        public Response method(String name, Entity<?> entity) {
            return builder.method(name, entity);
        }

        @Override
        public <T> T method(String name, Entity<?> entity, Class<T> responseType) {
            return builder.method(name, entity, responseType);
        }
        
        @Override
        public <T> T method(String name, Entity<?> entity, GenericType<T> responseType) {
            return builder.method(name, entity, responseType);
        }
    }
    
        

    /**
     * Completable JAX-RS AsyncInvoker
     */
    public static class RxInvoker implements AsyncInvoker {
        private final AsyncInvoker asyncInvoker;
        
        private RxInvoker(AsyncInvoker asyncInvoker) {
            this.asyncInvoker = asyncInvoker;
        }

        @Override
        public CompletableFuture<Response> get() {
            CompletableFutureCallbackAdapter adapter = new CompletableFutureCallbackAdapter();
            asyncInvoker.get(adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> get(Class<T> responseType) {
            CompletableFutureObjectCallbackAdapter<T> adapter = new CompletableFutureObjectCallbackAdapter<>(responseType);
            asyncInvoker.get(adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> get(GenericType<T> responseType) {
            CompletableFutureGenericObjectCallbackAdapter<T> adapter = new CompletableFutureGenericObjectCallbackAdapter<>(responseType);
            asyncInvoker.get(adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> Future<T> get(InvocationCallback<T> callback) {
            return asyncInvoker.get(callback);
        }

        @Override
        public CompletableFuture<Response> put(Entity<?> entity) {
            CompletableFutureCallbackAdapter adapter = new CompletableFutureCallbackAdapter();
            asyncInvoker.put(entity, adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> put(Entity<?> entity, Class<T> responseType) {
            CompletableFutureObjectCallbackAdapter<T> adapter = new CompletableFutureObjectCallbackAdapter<>(responseType);
            asyncInvoker.put(entity, adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> put(Entity<?> entity, GenericType<T> responseType) {
            CompletableFutureGenericObjectCallbackAdapter<T> adapter = new CompletableFutureGenericObjectCallbackAdapter<>(responseType);
            asyncInvoker.put(entity, adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> Future<T> put(Entity<?> entity, InvocationCallback<T> callback) {
            return asyncInvoker.put(entity, callback);
        }

        @Override
        public CompletableFuture<Response> post(Entity<?> entity) {
            CompletableFutureCallbackAdapter adapter = new CompletableFutureCallbackAdapter();
            asyncInvoker.post(entity, adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> post(Entity<?> entity, Class<T> responseType) {
            CompletableFutureObjectCallbackAdapter<T> adapter = new CompletableFutureObjectCallbackAdapter<>(responseType);
            asyncInvoker.post(entity, adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> post(Entity<?> entity, GenericType<T> responseType) {
            CompletableFutureGenericObjectCallbackAdapter<T> adapter = new CompletableFutureGenericObjectCallbackAdapter<>(responseType);
            asyncInvoker.post(entity, adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> Future<T> post(Entity<?> entity, InvocationCallback<T> callback) {
            return asyncInvoker.post(entity, callback);
        }

        @Override
        public CompletableFuture<Response> delete() {
            CompletableFutureCallbackAdapter adapter = new CompletableFutureCallbackAdapter();
            asyncInvoker.delete(adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> delete(Class<T> responseType) {
            CompletableFutureObjectCallbackAdapter<T> adapter = new CompletableFutureObjectCallbackAdapter<>(responseType);
            asyncInvoker.delete(adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> delete(GenericType<T> responseType) {
            CompletableFutureGenericObjectCallbackAdapter<T> adapter = new CompletableFutureGenericObjectCallbackAdapter<>(responseType);
            asyncInvoker.delete(adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> Future<T> delete(InvocationCallback<T> callback) {
            return asyncInvoker.delete(callback);
        }

        @Override
        public CompletableFuture<Response> head() {
            CompletableFutureCallbackAdapter adapter = new CompletableFutureCallbackAdapter();
            asyncInvoker.head(adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public Future<Response> head(InvocationCallback<Response> callback) {
            return asyncInvoker.head(callback);
        }

        @Override
        public CompletableFuture<Response> options() {
            CompletableFutureCallbackAdapter adapter = new CompletableFutureCallbackAdapter();
            asyncInvoker.options(adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> options(Class<T> responseType) {
            CompletableFutureObjectCallbackAdapter<T> adapter = new CompletableFutureObjectCallbackAdapter<>(responseType);
            asyncInvoker.options(adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> options(GenericType<T> responseType) {
            CompletableFutureGenericObjectCallbackAdapter<T> adapter = new CompletableFutureGenericObjectCallbackAdapter<>(responseType);
            asyncInvoker.options(adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> Future<T> options(InvocationCallback<T> callback) {
            return asyncInvoker.options(callback);
        }

        @Override
        public CompletableFuture<Response> trace() {
            CompletableFutureCallbackAdapter adapter = new CompletableFutureCallbackAdapter();
            asyncInvoker.trace(adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> trace(Class<T> responseType) {
            CompletableFutureObjectCallbackAdapter<T> adapter = new CompletableFutureObjectCallbackAdapter<>(responseType);
            asyncInvoker.trace(adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> trace(GenericType<T> responseType) {
            CompletableFutureGenericObjectCallbackAdapter<T> adapter = new CompletableFutureGenericObjectCallbackAdapter<>(responseType);
            asyncInvoker.trace(adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> Future<T> trace(InvocationCallback<T> callback) {
            return asyncInvoker.trace(callback);
        }

        @Override
        public CompletableFuture<Response> method(String name) {
            CompletableFutureCallbackAdapter adapter = new CompletableFutureCallbackAdapter();
            asyncInvoker.method(name, adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> method(String name, Class<T> responseType) {
            CompletableFutureObjectCallbackAdapter<T> adapter = new CompletableFutureObjectCallbackAdapter<>(responseType);
            asyncInvoker.method(name, adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> method(String name, GenericType<T> responseType) {
            CompletableFutureGenericObjectCallbackAdapter<T> adapter = new CompletableFutureGenericObjectCallbackAdapter<>(responseType);
            asyncInvoker.method(name, adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> Future<T> method(String name, InvocationCallback<T> callback) {
            return asyncInvoker.method(name, callback);
        }

        @Override
        public CompletableFuture<Response> method(String name, Entity<?> entity) {
            CompletableFutureCallbackAdapter adapter = new CompletableFutureCallbackAdapter();
            asyncInvoker.method(name, entity, adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> CompletableFuture<T> method(String name, Entity<?> entity, Class<T> responseType) {
            CompletableFutureObjectCallbackAdapter<T> adapter = new CompletableFutureObjectCallbackAdapter<>(responseType);
            asyncInvoker.method(name, entity, adapter);
            return adapter.getCompletableFuture();
        }
        
        @Override
        public <T> CompletableFuture<T> method(String name, Entity<?> entity, GenericType<T> responseType) {
            CompletableFutureGenericObjectCallbackAdapter<T> adapter = new CompletableFutureGenericObjectCallbackAdapter<>(responseType);
            asyncInvoker.method(name, entity, adapter);
            return adapter.getCompletableFuture();
        }

        @Override
        public <T> Future<T> method(String name, Entity<?> entity, InvocationCallback<T> callback) {
            return asyncInvoker.method(name, entity, callback);
        }
        
        
        private static class CompletableFutureCallbackAdapter implements InvocationCallback<Response> {
            private final CompletableFuture<Response> future = new CompletableFuture<Response>();
            
            CompletableFuture<Response> getCompletableFuture() {
                return future;
            }
            
            @Override
            public void completed(Response response) {
                future.complete(response);
            }
        
            @Override
            public void failed(Throwable t) {
                future.completeExceptionally(t);
            }
        }
        
        
        
        private static class CompletableFutureGenericObjectCallbackAdapter<T> implements InvocationCallback<Response> {
            private final GenericType<T> responseType;
            private final CompletableFuture<T> future = new CompletableFuture<T>();
            
            public CompletableFutureGenericObjectCallbackAdapter(GenericType<T> responseType) {
                this.responseType = responseType;
            }
            
            CompletableFuture<T> getCompletableFuture() {
                return future;
            }
            
            @Override
            public void completed(Response response) {
                try {
                    T object = extractResult(responseType, response, null);
                    future.complete(object);
                } catch (Throwable t) {
                    failed(t);
                    return;
                }
            }
        
            @Override
            public void failed(Throwable t) {
                future.completeExceptionally(t);
            }
            
            
            
            ///////////////////////////////////////
            // taken FROM RESTEASY and modified 
            
            private T extractResult(GenericType<T> responseType, Response response, Annotation[] annotations) {

               int status = response.getStatus();
               if (status >= 200 && status < 300) {
                   return response.readEntity(responseType);
                   
               } else {
                   try {
                      // Buffer the entity for any exception thrown as the response may have any entity the user wants
                      // We don't want to leave the connection open though.
                      response.bufferEntity();
                      
                      if (status >= 300 && status < 400) {
                          throw new RedirectionException(response);
                      } else {
                          return handleErrorStatus(response);
                      }
                      
                   } finally {
                      // close if no content
                      if (response.getMediaType() == null) {
                          response.close();
                      }
                   }
               }
            }

            
            private T handleErrorStatus(Response response) {
                final int status = response.getStatus();
                
                switch (status) {
                   case 400:
                      throw new BadRequestException(response);
                   case 401:
                      throw new NotAuthorizedException(response);
                   case 404:
                      throw new NotFoundException(response);
                   case 405:
                      throw new NotAllowedException(response);
                   case 406:
                      throw new NotAcceptableException(response);
                   case 415:
                      throw new NotSupportedException(response);
                   case 500:
                      throw new InternalServerErrorException(response);
                   case 503:
                      throw new ServiceUnavailableException(response);
                   default:
                      break;
                }

                if (status >= 400 && status < 500) {
                    throw new ClientErrorException(response);
                    
                } else if (status >= 500)  {
                    throw new ServerErrorException(response);
                    
                } else {
                    throw new WebApplicationException(response);
                }
             }
               
            // FROM RESTEASY 
            ///////////////////////////////////////
        }
        
        
        
        private static class CompletableFutureObjectCallbackAdapter<T> extends CompletableFutureGenericObjectCallbackAdapter<T> {
            
            public CompletableFutureObjectCallbackAdapter(Class<T> responseType) {
                super(new GenericType<T>(responseType));
            }
        }
    }
}