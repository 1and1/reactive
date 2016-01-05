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
package net.oneandone.incubator.neo.datareplicator;



import java.io.ByteArrayInputStream;

import java.io.Closeable;
import java.io.File;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;


final class ReplicationJobBuilderImpl implements ReplicationJobBuilder {    
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationJobBuilderImpl.class);
    
    private final URI uri;
    private final boolean failOnInitFailure;
    private final Duration refreshPeriod;
    private final File cacheDir;
    private final Duration maxCacheTime;
    private final Client client;
    
   
    ReplicationJobBuilderImpl(final URI uri, 
                       final boolean failOnInitFailure, 
                       final File cacheDir,
                       final Duration maxCacheTime,
                       final Duration refreshPeriod,
                       final Client client) {
        this.uri = uri;
        this.failOnInitFailure = failOnInitFailure;
        this.refreshPeriod = refreshPeriod;
        this.cacheDir = cacheDir;
        this.maxCacheTime = maxCacheTime;
        this.client = client;
    }

    @Override
    public ReplicationJobBuilderImpl withRefreshPeriod(final Duration refreshPeriod) {
        Preconditions.checkNotNull(refreshPeriod);
        return new ReplicationJobBuilderImpl(this.uri, 
                                      this.failOnInitFailure, 
                                      this.cacheDir, 
                                      this.maxCacheTime, 
                                      refreshPeriod,
                                      this.client);
    }
    
    @Override
    public ReplicationJobBuilderImpl withMaxCacheTime(final Duration maxCacheTime) {
        Preconditions.checkNotNull(maxCacheTime);
        return new ReplicationJobBuilderImpl(this.uri, 
                                      this.failOnInitFailure,
                                      this.cacheDir, 
                                      maxCacheTime,
                                      this.refreshPeriod,
                                      this.client);
    }
    
    @Override
    public ReplicationJobBuilderImpl withFailOnInitFailure(final boolean failOnInitFailure) {
        return new ReplicationJobBuilderImpl(this.uri,
                                      failOnInitFailure,
                                      this.cacheDir, 
                                      this.maxCacheTime, 
                                      this.refreshPeriod,
                                      this.client);
    }
    
    @Override
    public ReplicationJobBuilderImpl withCacheDir(final File cacheDir) {
        Preconditions.checkNotNull(cacheDir);
        return new ReplicationJobBuilderImpl(this.uri, 
                                      this.failOnInitFailure, 
                                      cacheDir, 
                                      this.maxCacheTime,
                                      this.refreshPeriod,
                                      this.client);
    }
    
    @Override
    public ReplicationJobBuilderImpl withClient(final Client client) {
        Preconditions.checkNotNull(client);
        return new ReplicationJobBuilderImpl(this.uri, 
                                      this.failOnInitFailure, 
                                      this.cacheDir,    
                                      this.maxCacheTime,
                                      this.refreshPeriod,
                                      client);
    }
    
    @Override
    public ReplicationJob startConsumingBinary(final Consumer<byte[]> consumer) {
        Preconditions.checkNotNull(consumer);
        return new ReplicatonJobImpl(uri, 
                                     failOnInitFailure, 
                                     cacheDir,
                                     maxCacheTime,
                                     refreshPeriod,
                                     client,
                                     consumer); 
    }
 
    @Override
    public ReplicationJob startConsumingText(final Consumer<String> consumer) {
        Preconditions.checkNotNull(consumer);
        return startConsumingBinary(binary -> consumer.accept(new String(binary, Charsets.UTF_8)));
    }
 
    @Override
    public ReplicationJob startConsumingTextList(final Consumer<ImmutableList<String>> consumer) {
        Preconditions.checkNotNull(consumer);
        return startConsumingText(text -> consumer.accept(ImmutableList.copyOf(Splitter.onPattern("\r?\n")
                                                                                       .trimResults()
                                                                                       .splitToList(text))));
    }
 
    
    private static final class ReplicatonJobImpl implements ReplicationJob {
        
        private final Datasource datasource; 
        private final FileCache fileCache;
        private final Consumer<byte[]> consumer;
        private final ScheduledExecutorService executor;
        private final Duration maxCacheTime;
        private final Duration refreshPeriod;
        
        private final AtomicReference<Optional<Instant>> lastRefreshSuccess = new AtomicReference<>(Optional.empty());
        private final AtomicReference<Optional<Instant>> lastRefreshError = new AtomicReference<>(Optional.empty());

        
        
        public ReplicatonJobImpl(final URI uri,
                                 final boolean failOnInitFailure, 
                                 final File cacheDir, 
                                 final Duration maxCacheTime,
                                 final Duration refreshPeriod, 
                                 final Client client,
                                 final Consumer<byte[]> consumer) {
            
            this.maxCacheTime = maxCacheTime;
            this.refreshPeriod = refreshPeriod;
            this.consumer = new HashProtectedConsumer(consumer);
            this.fileCache = new FileCache(cacheDir, uri.toString(), maxCacheTime);
            
            // create proper data source 
            if (uri.getScheme().equalsIgnoreCase("classpath")) {
                this.datasource = new ClasspathDatasource(uri);
                
            } else if (uri.getScheme().equalsIgnoreCase("http") || uri.getScheme().equalsIgnoreCase("https")) {
                this.datasource = new HttpDatasource(uri, client);
                
            } else if (uri.getScheme().equalsIgnoreCase("snapshot")) {
                this.datasource = new MavenSnapshotDatasource(uri, client);
                
            } else if (uri.getScheme().equalsIgnoreCase("file")) {
                this.datasource = new FileDatasource(uri);
             
            } else {
                throw new RuntimeException("scheme of " + uri + " is not supported (supported: classpath, http, https)");
            }

            
            
            // load on startup
            try {
                load();

            } catch (final RuntimeException rt) {
            
                if (failOnInitFailure) {
                    throw rt;
                    
                } else {
                    // try to load from cache 
                    try {
                        consumer.accept(fileCache.load().get());
                    } catch (RuntimeException rt2) {
                        throw rt;
                    }
                } 
            }
            
            
            // start scheduler
            this.executor = Executors.newScheduledThreadPool(0);
            executor.scheduleWithFixedDelay(() -> load(), refreshPeriod.toMillis(), refreshPeriod.toMillis(), TimeUnit.MILLISECONDS);
        }
    
        
        @Override
        public void close() {
            executor.shutdown();
            datasource.close();
        }
        
        
        private void load() {
            try {
                datasource.load().ifPresent(binary -> {
                                                        consumer.accept(binary); 
                                                        fileCache.update(binary);  // data has been accepted by the consumer -> update cache
                                                      });
                
                lastRefreshSuccess.set(Optional.of(Instant.now()));  // will be updated event though optional is empty
                
            } catch (RuntimeException rt) {
                LOG.warn("error occured by loading " + getEndpoint(), rt);
                lastRefreshError.set(Optional.of(Instant.now()));
                
                throw rt;
            }
        }
        
        
        @Override
        public URI getEndpoint() {
            return datasource.getEndpoint();
        }
       
        @Override
        public Duration getMaxCacheTime() {
            return maxCacheTime;
        }
        
        @Override
        public Duration getRefreshPeriod() {
            return refreshPeriod;
        }
        
        @Override
        public Optional<Duration> getExpiredTimeSinceRefreshSuccess() {
            return lastRefreshSuccess.get().map(time -> Duration.between(time, Instant.now()));
        }
        
        @Override
        public Optional<Duration> getExpiredTimeSinceRefreshError() {
            return lastRefreshError.get().map(time -> Duration.between(time, Instant.now()));
        }
        
        
        @Override
        public String toString() {
            return new StringBuilder(datasource.toString())
                            .append(", refreshperiod=").append(refreshPeriod)
                            .append(", maxCacheTime=").append(maxCacheTime)
                            .append(" (last reload success: ").append(lastRefreshSuccess.get().map(time -> time.toString()).orElse("none")) 
                            .append(", last reload error: ").append(lastRefreshError.get().map(time -> time.toString()).orElse("none")).append(")")
                            .toString();
        }
        
        
        
        private static final class HashProtectedConsumer implements Consumer<byte[]> {

            private final Consumer<byte[]> consumer;
            private final AtomicReference<Long> lastMd5 = new AtomicReference<>(Hashing.md5().newHasher().hash().asLong());
            
            public HashProtectedConsumer(final Consumer<byte[]> consumer) {
                this.consumer = consumer;
            }
            
            @Override
            public void accept(final byte[] binary) {
                final long md5 = Hashing.md5().newHasher().putBytes(binary).hash().asLong();
                if (md5 != lastMd5.get()) {
                    consumer.accept(binary);
                    lastMd5.set(md5);
                }
            }
        }
        
        
        private static abstract class Datasource implements Closeable {
            private final URI uri;
            
            public Datasource(URI uri) {
                this.uri = uri;
            }

            @Override
            public void close() { }
            
            public URI getEndpoint() {
                return uri;
            }
                        
            public abstract Optional<byte[]> load();
            
            @Override
            public String toString() {
                return "[" + this.getClass().getSimpleName() + "] uri=" + uri;
            }
        }
        
      
       
        private static class ClasspathDatasource extends Datasource {
            
            public ClasspathDatasource(final URI uri) {
                super(uri);
            }
            
            @Override
            public Optional<byte[]> load() {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                if (classLoader == null) {
                    classLoader = getClass().getClassLoader();
                }
                
                final URL classpathUri = classLoader.getResource(getEndpoint().getRawSchemeSpecificPart());           
                if (classpathUri == null) {
                    throw new RuntimeException("resource " + getEndpoint().getRawSchemeSpecificPart() + " not found in classpath");
                    
                } else {
                    InputStream is = null;
                    try {
                        return Optional.of(ByteStreams.toByteArray(classpathUri.openStream()));
                    } catch (IOException ioe) {
                        throw new DatasourceException(ioe);
                    } finally {
                        Closeables.closeQuietly(is);
                    }
                }
            }
        }

        
        

        private static class FileDatasource extends Datasource {
            
            public FileDatasource(URI uri) {
                super(uri);
            }
          
            @Override
            public Optional<byte[]> load() {
                final File file = new File(getEndpoint().getPath());
                if (file.exists()) {
                    try {
                        return Optional.of(Files.toByteArray(file));
                    } catch (IOException ioe) {
                        throw new DatasourceException(ioe);
                    }
                    
                } else {
                    throw new RuntimeException("file " + file.getAbsolutePath() + " not found");
                }
            }
        }

        
                
        
        private static class HttpDatasource extends Datasource {
            private final AtomicReference<String> etag = new AtomicReference<String>();
            private final Client client;
            private final boolean isUserClient;
            
            public HttpDatasource(final URI uri, final Client client) {
                super(uri);
                this.isUserClient = client != null;
                this.client = (client != null) ? client : ClientBuilder.newClient();
            }
            
            @Override
            public void close() {
                super.close();
                if (!isUserClient) {
                    client.close();
                }
            }
            
            @Override
            public Optional<byte[]> load() {
                return load(getEndpoint());
            }
                
            protected Optional<byte[]> load(URI uri) {
                
                Response response = null;
                try {

                    Builder builder = client.target(uri).request();
                    if (etag.get() != null) {
                        builder = builder.header("etag", etag.get());
                    }
                    response = builder.get();

                    int status = response.getStatus(); 
                    if ((status / 100) == 2) {
                        etag.set(response.getHeaderString("etag"));
                        return Optional.of(response.readEntity(byte[].class));
                        
                    // not modified
                    } else if (status == 304) {
                        return Optional.empty();
             
                    // other (client error, ...) 
                    } else {
                        throw new DatasourceException("got" + status + " by calling " + getEndpoint());
                    }
                
                } finally {
                    if (response != null) {
                        response.close();
                    }
                }
            }
        }

        
        
        private static class MavenSnapshotDatasource extends HttpDatasource {
            
            public MavenSnapshotDatasource(final URI uri, final Client client) {
                super(uri, client);
            }
            
            
            @Override
            public Optional<byte[]> load() {
                URI xmlUri = URI.create(getEndpoint().getSchemeSpecificPart() + "/maven-metadata.xml"); 
                return load(xmlUri).map(xmlFile -> parseNewesetSnapshotUri(getEndpoint().getSchemeSpecificPart(), xmlFile))
                                   .flatMap(newestSnapshotUri -> load(newestSnapshotUri));
            }
            
            
            private URI parseNewesetSnapshotUri(String uri, byte[] xmlFile) {
                try {
                    Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new ByteArrayInputStream(xmlFile));
                    
                    String artifactId = ((Element) document.getElementsByTagName("artifactId").item(0)).getTextContent();
                    String version = ((Element) document.getElementsByTagName("version").item(0)).getTextContent();
                    
                    Element node = (Element) document.getElementsByTagName("versioning").item(0);
                    node = (Element) node.getElementsByTagName("snapshot").item(0);
                    String timestamp = ((Element) node.getElementsByTagName("timestamp").item(0)).getTextContent();
                    String buildNumber = ((Element) node.getElementsByTagName("buildNumber").item(0)).getTextContent();
                    
                    return URI.create(uri + "/" + artifactId + "-" + version.replace("-SNAPSHOT", "-" + timestamp + "-" + buildNumber + ".jar"));
                     
                } catch (IOException | ParserConfigurationException | SAXException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        
        
        private static final class FileCache extends Datasource {
            private final String cacheFileName;
            private final Duration maxCacheTime;
            
            
            public FileCache(final File cacheDir, final String name, final Duration maxCacheTime) {
                this(new File(new File(cacheDir, "datareplicator"), name.replaceAll("[,:/]", "_")), maxCacheTime);
            }
                
            public FileCache(final File cacheFile, final Duration maxCacheTime) {
                super(cacheFile.toURI());

                this.maxCacheTime = maxCacheTime;
                
                cacheFile.getParentFile().mkdirs();
                cacheFileName = cacheFile.getAbsolutePath();
            }
            
            public void update(final byte[] data) {
                try {
                    final File tempFile = new File(cacheFileName + ".temp");
                    if (tempFile.exists()) {
                        tempFile.delete();
                    }
                    
                    FileOutputStream os = null;
                    try {
                        os = new FileOutputStream(tempFile);
                        os.write(data);
                        os.close();

                        
                        //////////
                        // if the process crashes after deleting the cacheFile and
                        // before renaming the tempFile into the cacheFile, the 
                        // cache file will be lost.
                        // However this does not matter and is very unlikely
                        
                        final File cacheFile = new File(cacheFileName);
                        if (cacheFile.exists()) {
                            cacheFile.delete();
                        }
                        tempFile.renameTo(cacheFile);
                        
                        //
                        ///////////
                        
                        
                    } finally {
                        Closeables.close(os, true);
                    }
                    
                    
                } catch (IOException ioe) {
                    LOG.warn("updating cache file " + cacheFileName + " failed", ioe);
                }
            }
             
            private Optional<Duration> getExpiredTimeSinceRefresh() {
                final File cacheFile = new File(cacheFileName);
                if (cacheFile.exists()) {
                    return Optional.of(Duration.between(Instant.ofEpochMilli(cacheFile.lastModified()), Instant.now()));
                } else  {
                    return Optional.empty();
                }
            }
   
            @Override
            public Optional<byte[]> load() {

                // check if cache file is expired
                final Duration age = getExpiredTimeSinceRefresh().orElseThrow(() -> new DatasourceException("no cache entry exists"));
                if (maxCacheTime.minus(age).isNegative()) {
                    throw new DatasourceException("cache file is expired. Age is " + age.toDays() + " days");
                }
                
                // if not load it 
                final File cacheFile = new File(cacheFileName);
                if (cacheFile.exists()) {
                    FileInputStream is = null;
                    try {
                        is = new FileInputStream(cacheFile);
                        return Optional.of(ByteStreams.toByteArray(is));
                    } catch (IOException ioe) {
                        throw new DatasourceException("loading cache file " + cacheFileName + " failed", ioe);
                    } finally {
                        Closeables.closeQuietly(is);
                    }
                } else {
                    throw new DatasourceException("no cache file exists");
                }
            }
        }        
    }
}