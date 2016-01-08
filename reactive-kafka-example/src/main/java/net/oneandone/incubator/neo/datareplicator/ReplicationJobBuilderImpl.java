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
import java.util.UUID;
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
import com.google.common.base.Strings;
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
            this.consumer = new ConsumerAdapter(consumer);
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
                throw new RuntimeException("scheme of " + uri + " is not supported (supported: classpath, http, https, file)");
            }


            // load on startup
            try {
                loadAndNotifyConsumer();
                
            } catch (final RuntimeException rt) {
                if (failOnInitFailure) {
                    throw rt;
                } else {
                    // fallback -> try to load from cache (will throw a runtime exception, if fails)
                    notifyConsumer(fileCache.load());  
                } 
            }
            
            
            // start scheduler for periodically reloadings
            this.executor = Executors.newScheduledThreadPool(0);
            executor.scheduleWithFixedDelay(() -> loadAndNotifyConsumer(), 
                                            refreshPeriod.toMillis(),
                                            refreshPeriod.toMillis(), 
                                            TimeUnit.MILLISECONDS);
        }
        
        @Override
        public void close() {
            executor.shutdown();
            datasource.close();
        }
        
        private void loadAndNotifyConsumer() {
            try {
                byte[] data = datasource.load();
                notifyConsumer(data);

                // data has been accepted by the consumer -> update cache
                fileCache.update(data); 
                lastRefreshSuccess.set(Optional.of(Instant.now()));  
                
            } catch (final RuntimeException rt) {
                // loading failed or consumer has not accepted the data 
                LOG.warn("error occured by loading " + getEndpoint(), rt);
                lastRefreshError.set(Optional.of(Instant.now()));
                
                throw rt;
            }
        }
        
        private void notifyConsumer(final byte[] data) {
            consumer.accept(data);   // let the consumer handle the new data
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
        
        
        
        private static final class ConsumerAdapter implements Consumer<byte[]> {
            private final Consumer<byte[]> consumer;
            private final AtomicReference<Long> lastMd5 = new AtomicReference<>(Hashing.md5().newHasher().hash().asLong());
            
            public ConsumerAdapter(final Consumer<byte[]> consumer) {
                this.consumer = consumer;
            }
            
            @Override
            public void accept(final byte[] binary) {
                final long md5 = Hashing.md5().newHasher().putBytes(binary).hash().asLong();
                
                // data changed (new or modified)?   
                if (md5 != lastMd5.get()) {
                    // yes   
                    consumer.accept(binary);
                    lastMd5.set(md5);
                }
            }
        }
        
        
        private static abstract class Datasource implements Closeable {
            private final URI uri;
            
            public Datasource(final URI uri) {
                this.uri = uri;
            }

            @Override
            public void close() { }
            
            public URI getEndpoint() {
                return uri;
            }
                        
            public abstract byte[] load() throws ReplicationException;
            
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
            public byte[] load() throws ReplicationException {
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
                        return ByteStreams.toByteArray(classpathUri.openStream());
                    } catch (final IOException ioe) {
                        throw new ReplicationException(ioe);
                    } finally {
                        Closeables.closeQuietly(is);
                    }
                }
            }
        }

        
        

        private static class FileDatasource extends Datasource {
            
            public FileDatasource(final URI uri) {
                super(uri);
            }
          
            @Override
            public byte[] load() {
                final File file = new File(getEndpoint().getPath());
                if (file.exists()) {
                    try {
                        return Files.toByteArray(file);
                    } catch (IOException ioe) {
                        throw new ReplicationException(ioe);
                    }
                    
                } else {
                    throw new ReplicationException("file " + file.getAbsolutePath() + " not found");
                }
            }
        }

        
                
        
        private static class HttpDatasource extends Datasource {
            private final Client client;
            private final boolean isUserClient;
            private final AtomicReference<CachedResponseData> cacheResponseDate = new AtomicReference<>(CachedResponseData.EMPTY);
            
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
            public byte[] load() {
                return load(getEndpoint());
            }
                
            protected byte[] load(final URI uri) {
                Response response = null;
                try {
                    
                    CachedResponseData cached = cacheResponseDate.get();
                    if (!cached.getUri().equals(uri)) {
                        cached = null;
                    }
                    
                    
                    Builder builder = client.target(uri).request();

                    // will make request conditional, if a response has already been a received 
                    if (cached != null) {
                        builder = builder.header("etag", cached.getEtag());
                    }
                    

                    // perform query 
                    response = builder.get();
                    final int status = response.getStatus();
                    
                    
                    // success
                    if ((status / 100) == 2) {
                        final byte[] data = response.readEntity(byte[].class);
                        
                        final String etag = response.getHeaderString("etag");
                        if (!Strings.isNullOrEmpty(etag)) {
                            // add response to cache
                            cacheResponseDate.set(new CachedResponseData(uri, data, etag));
                        }
                        
                        return data;
                        
                    // not modified
                    } else if (status == 304) {
                        if (cached == null) {
                            throw new ReplicationException("got " + status + " by performing non-conditional request " + getEndpoint());
                        } else {
                            return cached.getData();
                        }
             
                    // other (client error, ...) 
                    } else {
                        throw new ReplicationException("got " + status + " by calling " + getEndpoint());
                    }
                
                } finally {
                    if (response != null) {
                        response.close();
                    }
                }
            }
            
            
            private static class CachedResponseData {
                final static CachedResponseData EMPTY = new CachedResponseData(URI.create("http://example.org"), new byte[0], ""); 
                
                private final URI uri;
                private final byte[] data;
                private final String etag;
                
                public CachedResponseData(final URI uri, final byte[] data, final String etag) {
                    this.uri = uri;
                    this.data = data;
                    this.etag = etag;
                }
                
                public URI getUri() {
                    return uri;
                }
                
                public String getEtag() {
                    return etag;
                }
                
                public byte[] getData() {
                    return data;
                }
            }
        }

        
        
        // MavenSnapshotDatasource will be removed. Implemented for test purposes only 
        private static class MavenSnapshotDatasource extends HttpDatasource {
            
            public MavenSnapshotDatasource(final URI uri, final Client client) {
                super(uri, client);
            }
            
            
            @Override
            public byte[] load() {
                final byte[] xmlFile = load(URI.create(getEndpoint().getSchemeSpecificPart() + "/maven-metadata.xml"));
                return load(parseNewestSnapshotUri(getEndpoint().getSchemeSpecificPart(), xmlFile));
            }
            
            
            private URI parseNewestSnapshotUri(final String uri, final byte[] xmlFile) {
                try {
                    final Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new ByteArrayInputStream(xmlFile));
                    
                    final String artifactId = ((Element) document.getElementsByTagName("artifactId").item(0)).getTextContent();
                    final String version = ((Element) document.getElementsByTagName("version").item(0)).getTextContent();
                    
                    Element node = (Element) document.getElementsByTagName("versioning").item(0);
                    node = (Element) node.getElementsByTagName("snapshot").item(0);
                    final String timestamp = ((Element) node.getElementsByTagName("timestamp").item(0)).getTextContent();
                    final String buildNumber = ((Element) node.getElementsByTagName("buildNumber").item(0)).getTextContent();
                    
                    return URI.create(uri + "/" + artifactId + "-" + version.replace("-SNAPSHOT", "-" + timestamp + "-" + buildNumber + ".jar"));
                     
                } catch (final IOException | ParserConfigurationException | SAXException e) {
                    throw new ReplicationException(e);
                }
            }
        }

        
        
        private static final class FileCache extends Datasource {
            private final File cacheDir;
            private final String genericCacheFileName;
            private final Duration maxCacheTime;

            
            public FileCache(final File cacheDir, final String name, final Duration maxCacheTime) {
                super(cacheDir.toURI());

                try {
                    this.maxCacheTime = maxCacheTime;
                    this.genericCacheFileName =  name.replaceAll("[,:/]", "_") + "_";
                    this.cacheDir = new File(cacheDir, "datareplicator").getCanonicalFile();
                } catch (final IOException ioe) {
                    throw new ReplicationException(ioe);
                }
            }
            
            
            public void update(final byte[] data) {
                // creates a new cache file with timestamp
                final File cacheFile = new File(cacheDir, genericCacheFileName + Instant.now().toEpochMilli());
                cacheFile.getParentFile().mkdirs();
                final File tempFile = new File(cacheDir, UUID.randomUUID().toString() + ".temp");
                tempFile.getParentFile().mkdirs();
                
                /////
                // why this "newest cache file" approach?
                // this approach follows the immutable pattern and avoids race conditions by updating existing files. Instead
                // updating the cache file which could cause trouble in the case of concurrent processes, new cache files will
                // be written by using a timestamp as part of the file name.
                //
                // The code below makes sure that the newest cache file will never been deleted by concurrent processes
                // In worst case (-> crashed processes) dead, expired cache files could exist within the cache dir. However,
                // this does not matter
                ////
                
                try {
                    final Optional<File> previousCacheFile = getNewestCacheFile();
                    
                    FileOutputStream os = null;
                    try {
                        // write the new cache file 
                        os = new FileOutputStream(tempFile);
                        os.write(data);
                        os.close();

                        final boolean isNewCacheFileCommitted = tempFile.renameTo(cacheFile);
                         
                         
                        // and try to remove previous one
                        if (isNewCacheFileCommitted) {
                            previousCacheFile.ifPresent(previous -> previous.delete());
                        }
                    } finally {
                        Closeables.close(os, true);  // close os in any case 
                        tempFile.delete();  // make sure that temp file will be deleted even though something failed 
                    }
                    
                } catch (final IOException ioe) {
                    LOG.warn("writing cache file " + cacheFile.getAbsolutePath() + " failed", ioe);
                }
            }

          
            @Override
            public byte[] load() {

                final Optional<File> cacheFile = getNewestCacheFile();
                if (cacheFile.isPresent()) {
                    FileInputStream is = null;
                    try {
                        is = new FileInputStream(cacheFile.get());
                        return ByteStreams.toByteArray(is);
                    } catch (final IOException ioe) {
                        throw new ReplicationException("loading cache file " + cacheFile.get()  + " failed", ioe);
                    } finally {
                        Closeables.closeQuietly(is);
                    }
                    
                } else {
                    throw new ReplicationException("cache file not exists");
                }
            }
            
            
            private Optional<File> getNewestCacheFile() {
                long newestTimestamp = 0;
                File newestCacheFile = null;
                
                // find newest cache file 
                for (File file : cacheDir.listFiles()) {
                    final String fileName = file.getName();  
                    if (fileName.startsWith(genericCacheFileName)) {
                        try {
                            final long timestamp = Long.parseLong(fileName.substring(fileName.lastIndexOf("_") + 1, fileName.length()));
                            if (timestamp > newestTimestamp) {
                                newestCacheFile = file;
                                newestTimestamp = timestamp;
                            }
                        } catch (NumberFormatException nfe) {
                            LOG.debug(cacheDir.getAbsolutePath() + " contains cache file with invalid name " + fileName + " Ignoring it");
                        }
                    }
                }
                 
                
                // check if newest cache file is expired
                final Duration age = Duration.between(Instant.ofEpochMilli(newestCacheFile.lastModified()), Instant.now());
                if (maxCacheTime.minus(age).isNegative()) {
                    LOG.warn("cache file is expired. Age is " + age.toDays() + " days. Ignoring it");
                    newestCacheFile = null;
                }
                
                return Optional.ofNullable(newestCacheFile);
            }
        }        
    }
}