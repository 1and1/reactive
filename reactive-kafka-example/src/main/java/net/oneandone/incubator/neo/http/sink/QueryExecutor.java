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
package net.oneandone.incubator.neo.http.sink;

import java.io.Closeable;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.Invocation.Builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.incubator.neo.exception.Exceptions;
import net.oneandone.incubator.neo.http.sink.HttpSink.Method;


/**
 * Query executor
 */
class QueryExecutor implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutor.class);
    
	private final Client httpClient;
	private final ScheduledThreadPoolExecutor executor;
	private final AtomicBoolean isOpen = new AtomicBoolean(true);
	
	
	/**
	 * @param httpClient          the http client
	 * @param numParallelWorkers  the number of workers
	 */
	public QueryExecutor(final Client httpClient, final int numParallelWorkers) {
        this.executor = new ScheduledThreadPoolExecutor(numParallelWorkers);
		this.httpClient = httpClient;
	}
	
	@Override
	public void close()  {
		if (isOpen.getAndSet(false)) {
			executor.shutdown();
		}
	}
	
	public boolean isOpen() {
		return isOpen.get();
	}
     
	/**
	 * performs the query 
	 * @param id         the query id to log
	 * @param method     the method
	 * @param target     the target uri
	 * @param entity     the entity
	 * @param delay      the delay 
	 * @return the response body future
	 */
	public CompletableFuture<String> performHttpQueryAsync(final String id,
													       final Method method, 
													       final URI target, 
													       final Entity<?> entity,
													       final Duration delay) {
		if (!isOpen()) {
			throw new IllegalStateException("processor is already closed");
		}
		        		
    	final CompletablePromise<String> completablePromise = new CompletablePromise<>();
    	executor.schedule(() -> performHttpQueryNowAsync(id, method, target, entity).whenComplete(completablePromise),
        			      delay.toMillis(), 
        			      TimeUnit.MILLISECONDS);
        return completablePromise;

	}
	
	private CompletableFuture<String> performHttpQueryNowAsync(final String id,
															   final Method method, 
													  		   final URI target, 
													  		   final Entity<?> entity) {
        LOG.debug("performing " + id);
		final InvocationPromise promise = new InvocationPromise();
		
		try {
			final Builder builder = httpClient.target(target).request();
			if (method == Method.POST) {
				builder.async().post(entity, promise);
			} else {
				builder.async().put(entity, promise);
			}
		} catch (RuntimeException rt) {
			promise.completeExceptionally(rt);
		}     
            
		return promise;
	}

	private static final class InvocationPromise extends CompletableFuture<String> implements InvocationCallback<String> {
            
		@Override
		public void failed(final Throwable ex) {
			completeExceptionally(Exceptions.propagate(ex));
		}

		@Override
		public void completed(String content) {
			complete(content);
		}
	}
	
	
    private static final class CompletablePromise<R> extends CompletableFuture<R> implements BiConsumer<R, Throwable> {
		
		@Override
		public void accept(R result, Throwable error) {
			if (error == null) {
				complete(result);
			} else {
				completeExceptionally(error);
			}
		}
	}
}