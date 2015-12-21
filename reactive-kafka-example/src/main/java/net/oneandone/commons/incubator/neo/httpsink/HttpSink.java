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
package net.oneandone.commons.incubator.neo.httpsink;

import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.ResponseProcessingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import net.oneandone.commons.incubator.neo.collect.Immutables;


/**
 * the http sink provides a consumer-styled API for on-way HTTP upload
 * transactions. By performing such transaction data will be send to the server
 * without getting data back. The http sink provides auto-retries. <br>
 * <br>
 * 
 * Example with in-memory auto-retry
 * 
 * <pre>
 * EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics")
 *                               .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), 
 *                                                                Duration.ofSeconds(3), 
 *                                                                Duration.ofMinutes(1), 
 *                                                                Duration.ofMinutes(30),
 *                                                                Duration.ofHours(5)))
 *                               .open();
 * 
 *  // tries to send data. If fails (5xx) it will be auto-retried. In case of 
 *  // an 4xx an BadRequestException will be thrown 
 *  sink.acceptAsync(newCustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json").get(); 
 *  // ...
 *  
 *  sink.close();
 * <pre>
 * <br>
 * <br>
 * 
 * Example with persistent file-based memory auto-retry
 * 
 * <pre>
 * EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics")
 *                               .withRetryAfter(ImmutableList.of(Duration.ofMillis(100),
 *                                                                Duration.ofSeconds(3), 
 *                                                                Duration.ofMinutes(1),
 *                                                                Duration.ofMinutes(30),
 *                                                                Duration.ofHours(5)))
 *                               .withRetryPersistency(myRetryDir) 
 *                               .open();
 * 
 * // tries to send data. If fails (5xx) it will be auto-retried. In case of an
 * // 4xx an BadRequestException will be thrown 
 * sink.acceptAsync(newCustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json").get();
 * // ...
 * 
 * sink.close();
 * <pre>
 *
 */
public class HttpSink {
    private static final Logger LOG = LoggerFactory.getLogger(HttpSink.class);

    public enum Method {
        POST, PUT
    };

    private final URI target;
    private final Client client;
    private final Method method;
    private final int bufferSize;
    private final File dir;
    private final Optional<String> appId;
    private final ImmutableList<Duration> remainingRetries;
    private final int numParallelWorkers;
    private final BiConsumer<Object, String> discardListener;

    private HttpSink(final Client client, 
                     final URI target, 
                     final Method method, 
                     final int bufferSize, 
                     final File dir, 
                     final Optional<String> appId,
                     final ImmutableList<Duration> remainingRetries, 
                     final int numParallelWorkers,
                     final BiConsumer<Object, String> discardListener) {
        this.client = client;
        this.target = target;
        this.method = method;
        this.bufferSize = bufferSize;
        this.dir = dir;
        this.appId = appId;
        this.remainingRetries = remainingRetries;
        this.numParallelWorkers = numParallelWorkers;
        this.discardListener = discardListener;
    }

    /**
     * @param target the target uri
     * @return a new instance of the http sink
     */
    public static HttpSink create(final String target) {
        return create(URI.create(target));
    }

    /**
     * @param target the target uri
     * @return a new instance of the http sink
     */
    public static HttpSink create(final URI target) {
        return new HttpSink(null, 
                            target, 
                            Method.POST, 
                            Integer.MAX_VALUE,
                            null, 
                            Optional.empty(),
                            ImmutableList.of(),
                            1,
                            (object, mimeType) -> { LOG.warn("no retries left! discard query with " + object); });
    }

    /**
     * @param client the client to use
     * @return a new instance of the http sink
     */
    public HttpSink withClient(final Client client) {
        return new HttpSink(client, 
                            this.target, 
                            this.method, 
                            this.bufferSize, 
                            this.dir, 
                            this.appId,
                            this.remainingRetries, 
                            this.numParallelWorkers,
                            this.discardListener);
    }

    /**
     * @param method the method. Supported are POST and PUT (default is POST)
     * @return a new instance of the http sink
     */
    public HttpSink withMethod(final Method method) {
        return new HttpSink(this.client, 
                            this.target, method, 
                            this.bufferSize, 
                            this.dir,
                            this.appId,
                            this.remainingRetries,
                            this.numParallelWorkers,
                            this.discardListener);
    }

    /**
     * @param retryPauses the delays before retrying (Default is empty list)
     * @return a new instance of the http sink
     */
    public HttpSink withRetryAfter(final ImmutableList<Duration> retryPauses) {
        return new HttpSink(this.client, 
                            this.target, 
                            this.method,
                            this.bufferSize, 
                            this.dir, 
                            this.appId,
                            retryPauses,
                            this.numParallelWorkers, 
                            this.discardListener);
    }

    /**
     * @param numParallelWorkers the parallelity by performing retries (default is 1)
     * @return a new instance of the http sink
     */
    public HttpSink withRetryParallelity(final int numParallelWorkers) {
        return new HttpSink(this.client,
                            this.target, 
                            this.method, 
                            this.bufferSize,
                            this.dir,
                            this.appId,
                            this.remainingRetries,
                            numParallelWorkers,
                            this.discardListener);
    }

    /**
     * @param bufferSize the retry buffer size. If the size is exceeded, new retry jobs will be discarded (default is unlimited)
     * @return a new instance of the http sink
     */
    public HttpSink withRetryBufferSize(final int bufferSize) {
        return new HttpSink(this.client, 
                            this.target, 
                            this.method, 
                            bufferSize, 
                            this.dir,
                            this.appId,
                            this.remainingRetries,
                            this.numParallelWorkers, 
                            this.discardListener);
    }

    /**
     * @param dir the directory where the retry jobs will be stored. If not set,
     *            the retry jobs will be stored in-memory (default is unset)
     * @return a new instance of the http sink
     */
    public HttpSink withRetryPersistency(final File dir) {
        return new HttpSink(this.client, 
                            this.target, 
                            this.method, 
                            this.bufferSize, 
                            dir,
                            this.appId,
                            this.remainingRetries, 
                            this.numParallelWorkers, 
                            this.discardListener);
    }

    /**
     * @param discardListener  the discard listener
     * @return a new instance of the http sink
     */
    public HttpSink withDiscardListener(final  BiConsumer<Object, String> discardListener) {
        return new HttpSink(this.client,
                            this.target, 
                            this.method, 
                            this.bufferSize,
                            this.dir, 
                            this.appId,
                            this.remainingRetries,
                            this.numParallelWorkers,
                            discardListener);
    }

    /**
     * @return the sink reference
     */
    public EntityConsumer open() {
        return (dir == null) ? new InMemoryQueryQueue() : new FileQueryQueue(dir);
    }

    private boolean isPersistentQueue() {
        return dir != null;
    }

    private abstract class QueryQueue implements EntityConsumer {

        private final AtomicReference<Client> defaultClientRef = new AtomicReference<>();

        // statistics
        private final AtomicLong success = new AtomicLong(0);
        private final AtomicLong retries = new AtomicLong(0);
        private final AtomicLong discarded = new AtomicLong(0);

        @Override
        public long getNumSuccess() {
            return success.get();
        }

        @Override
        public long getNumRetries() {
            return retries.get();
        }

        @Override
        public long getNumDiscarded() {
            return discarded.get();
        }

        @Override
        public void close() {
            final Client defaultClient = defaultClientRef.get();
            if (defaultClient != null) {
                defaultClient.close();
            }
        }

        private Client getClient() {
            if (client == null) {
                synchronized (this) {
                    Client defaultClient = defaultClientRef.get();
                    if (defaultClient == null) {
                        defaultClient = ClientBuilder.newClient();
                        defaultClientRef.set(defaultClient);
                    }
                    return defaultClient;
                }
            } else {
                return client;
            }
        }

        @Override
        public CompletableFuture<Boolean> acceptAsync(final Object entity, final String mediaType) {
            if (isPersistentQueue() && !(mediaType.startsWith("application") && mediaType.endsWith("json"))) {
                throw new IllegalStateException("persistent retry supports JSON types only");
            }

            final CompletableFuture<Boolean> promise = new CompletableFuture<>();
            final Query query = new Query(UUID.randomUUID().toString(), target, method, appId,
                    Entity.entity(entity, mediaType), Duration.ZERO, remainingRetries);

            execute(query, promise);

            return promise;
        }

        private void execute(final Query query, final CompletableFuture<Boolean> promise) {
            query.executeWith(getClient()).whenComplete((Void, error) -> {
                                        try {
                        
                                            if (error == null) {
                                                success.incrementAndGet();
                                                promise.complete(true);
                                                onCompleted(query);
                        
                                            } else if (error instanceof ClientErrorException) {
                                                LOG.debug("got client error complete exceptionally", error);
                                                promise.completeExceptionally(error);
                        
                                            } else {
                                                LOG.warn("query failed: " + query);
                                                if (query.getRetryQuery().isPresent()) {
                                                    enqueue(query.getRetryQuery().get());
                                                    
                                                } else {
                                                    discarded.incrementAndGet();
                                                    onDiscard(query);
                                                    
                                                    synchronized (discardListener) {
                                                        discardListener.accept(query.getEntity().getEntity(),
                                                                               query.getEntity().getMediaType().toString());
                                                    }
                                                }
                                                promise.complete(false);
                                            }
                        
                                        } catch (RuntimeException rt) {
                                            promise.completeExceptionally(rt);
                                        }
            });
        }

        protected void executeRetry(final Query query) {
            retries.incrementAndGet();
            execute(query, new CompletableFuture<>());
        }

        protected abstract void enqueue(Query query);

        protected void onCompleted(Query query) { }
        
        protected void onDiscard(Query query) { }
    }

    private class InMemoryQueryQueue extends QueryQueue {
        private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(numParallelWorkers);

        @Override
        protected void enqueue(final Query query) {
            LOG.warn("schedule retry query " + query + " after " + query.getDelayBeforeExecute());
            executor.schedule(() -> executeRetry(query), 
                              query.getDelayBeforeExecute().toMillis(),
                              TimeUnit.MILLISECONDS);
        }

        @Override
        public int getQueueSize() {
            return executor.getActiveCount();
        }

        @Override
        public void close() {
            executor.shutdown();
            super.close();
        }
    }

    private class FileQueryQueue extends InMemoryQueryQueue {
        private final File retryDir;
        
        /*
         * TODO make it much more robust -> parallel processes, object deserialization is not necessary, 
         */

        public FileQueryQueue(File retryDir) {
            retryDir.mkdirs();
            this.retryDir = retryDir;

            try {
                for (File file : retryDir.listFiles()) {
                    if (file.getName().endsWith(".query")) {
                        try (FileInputStream fis = new FileInputStream(file)) {
                            super.enqueue(Query.readFrom(fis));
                        }
                    }
                }
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        @Override
        protected void enqueue(Query query) {
            try {
                File queryFile = getFile(query);
                try (FileOutputStream fos = new FileOutputStream(queryFile)) {
                    query.writeTo(fos);
                }

                super.enqueue(query);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }

        @Override
        protected void onCompleted(Query query) {
            super.onCompleted(query);
            getFile(query).delete();
        }

        @Override
        protected void onDiscard(Query query) {
            super.onDiscard(query);
            getFile(query).delete();
        }
        
        private File getFile(Query query) {
            return new File(retryDir, query.getId() + ".query");
        }

        @Override
        public int getQueueSize() {
            return 0;
        }

        @Override
        public void close() {
            super.close();
        }
    }

    private static final class Query {
        private final String id;
        private final Method method;
        private final URI target;
        private final Optional<String> appId;
        private final Entity<?> entity;
        private final Duration delayBeforeExecution;
        private final ImmutableList<Duration> remainingRetrys;

        public Query(final String id, final URI target, final Method method, final Optional<String> appId,
                Entity<?> entity, final Duration delayBeforeExecution, final ImmutableList<Duration> remainingRetrys) {
            this.id = id;
            this.target = target;
            this.method = method;
            this.appId = appId;
            this.entity = entity;
            this.delayBeforeExecution = delayBeforeExecution;
            this.remainingRetrys = remainingRetrys;
        }

        public String getId() {
            return id;
        }

        public Entity<?> getEntity() {
            return entity;
        }

        public Optional<Query> getRetryQuery() {
            if (remainingRetrys.isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(new Query(id, target, method, appId, entity, remainingRetrys.get(0),
                        remainingRetrys.subList(1, remainingRetrys.size())));
            }
        }

        public Duration getDelayBeforeExecute() {
            return delayBeforeExecution;
        }

        public CompletableFuture<Void> executeWith(Client client) {
            final ResponseHandler responseHandler = new ResponseHandler();

            final Builder builder = client.target(target).request();
            appId.ifPresent(id -> builder.header("X-APP", id));

            if (method == Method.POST) {
                builder.async().post(entity, responseHandler);
            } else {
                builder.async().put(entity, responseHandler);
            }

            return responseHandler;
        }

        void writeTo(OutputStream os) throws IOException {
            CharArrayWriter bos = new CharArrayWriter();
            new ObjectMapper().writeValue(bos, entity.getEntity());

            Writer writer = new OutputStreamWriter(os);
            writer.write("V1" + "\r\n");
            writer.write(id + "\r\n");
            writer.write(appId.orElse("null") + "\r\n");
            writer.write(method.toString() + "\r\n");
            writer.write(target + "\r\n");
            writer.write(delayBeforeExecution.toMillis() + "\r\n");
            writer.write(entity.getMediaType().toString() + "\r\n");
            writer.write(entity.getEntity().getClass().getName() + "\r\n");
            writer.write(bos.toCharArray().length + "\r\n");
            writer.write(bos.toCharArray());
            writer.write("\r\n");
            writer.write(Joiner.on("&").join(remainingRetrys.stream().map(duration -> duration.toMillis()).collect(Immutables.toList())));

            writer.flush();
        }

        static Query readFrom(InputStream is) throws IOException {
            try {
                LineNumberReader lnr = new LineNumberReader(new InputStreamReader(is));
                
                lnr.readLine();
                String id = lnr.readLine();
                String ai = lnr.readLine();
                Optional<String> appId = ai.equals("null") ? Optional.empty() : Optional.of(ai);
                Method method = Method.valueOf(lnr.readLine());
                URI target = URI.create(lnr.readLine());
                Duration delayBeforeExecution = Duration.ofMillis(Long.parseLong(lnr.readLine()));
                String mimeType = lnr.readLine();
                String classname = lnr.readLine();
                int length = Integer.parseInt(lnr.readLine());
                char[] data = new char[length];
                lnr.read(data);
                lnr.readLine();
                ImmutableList<Duration> remainingRetrys = Splitter.on('&')
                                                                  .splitToList(lnr.readLine())
                                                                  .stream()
                                                                  .map(millis -> Duration.ofMillis(Long.parseLong(millis)))
                                                                  .collect(Immutables.toList());
                
                // hould not necessary by performing the rest call the raw data could be used
                Object entity = new ObjectMapper().readValue(new String(data), Class.forName(classname));
                
                return new Query(id, target, method, appId, Entity.entity(entity, mimeType), delayBeforeExecution, remainingRetrys);
            } catch (ClassNotFoundException cnf) {
                throw new IOException(cnf);
            }
        }

        @Override
        public String toString() {
            return method + " " + target.toString();
        }

        private static final class ResponseHandler extends CompletableFuture<Void>
                implements InvocationCallback<String> {

            @Override
            public void failed(Throwable ex) {
                if (ex instanceof ResponseProcessingException) {
                    ex = ((ResponseProcessingException) ex).getCause();
                }
                completeExceptionally(ex);
            }

            @Override
            public void completed(String responseEntity) {
                complete(null);
            }
        }
    }
}
