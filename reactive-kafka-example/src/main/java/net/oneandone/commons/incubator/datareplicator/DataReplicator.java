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
package net.oneandone.commons.incubator.datareplicator;



import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;




/**
 * The data replicator replicates data periodically from uri base resources. Data will be cached locally for availability reasons.
 * Each time updated data is fetched the registered consumer will be called. 
 *
 */
public class DataReplicator {
    
    private static final Logger LOG = LoggerFactory.getLogger(DataReplicator.class);
    
    private final URI uri;
    private final boolean failOnInitFailure;
    private final Duration refreshPeriod;
    private final File cacheDir;
    private final String appId;
    private final Duration maxCacheTime;
    
    
    
    /**
     * @param uri  the source uri. Supported schemes are <i>file</i>, <i>http</i>, <i>https</i> as well as <i>classpath</i> 
     *             (e.g. file:/C:/dev/workspace/reactive2/reactive-kafka-example/src/main/resources/schemas.zip, 
     *              classpath:schemas/schemas.zip, http://myserver/schemas.zip)  
     */
    public DataReplicator(URI uri) {
        this(uri, 
             false, 
             new File("."), 
             Duration.ofDays(14),
             "datareplicator/1.0",
             Duration.ofSeconds(59));
    }
    
    private DataReplicator(URI uri, 
                           boolean failOnInitFailure, 
                           File cacheDir,
                           Duration maxCacheTime,
                           String appId,
                           Duration refreshPeriod) {
        this.uri = uri;
        this.failOnInitFailure = failOnInitFailure;
        this.refreshPeriod = refreshPeriod;
        this.cacheDir = cacheDir;
        this.appId = appId;
        this.maxCacheTime = maxCacheTime;
    }

    
    /**
     * @param refreshPeriod   the refresh period (default is 59 sec) 
     * @return the new instance of the data replicator
     */
    public DataReplicator withRefreshPeriod(Duration refreshPeriod) {
        return new DataReplicator(this.uri, 
                                  this.failOnInitFailure, 
                                  this.cacheDir, 
                                  this.maxCacheTime, 
                                  this.appId,
                                  refreshPeriod);
    }
    
    /**
     * 
     * @param maxCacheTime  the max cache time. after this time the cache entries will be ignored (default is 14 days)
     * @return the new instance of the data replicator
     */
    public DataReplicator withMaxCacheTime(Duration maxCacheTime) {
        return new DataReplicator(this.uri, 
                                  this.failOnInitFailure,
                                  this.cacheDir, 
                                  maxCacheTime,
                                  this.appId,
                                  this.refreshPeriod);
    }
    
    
    /**
     * Sets the value whether the application should terminate the start-up process when the data (source and local copy) 
     * are not available.<p>
     * 
     * If failOnInitFailure==true then {@link #activate()} immediately aborts with a RuntimeException if the configured
     * source cannot be fetched.
     * <p>
     * If failOnInitFailure==false and the source is unreachable: If a cached file exists, this file will be used. 
     * 
     * @param failOnInitFailure true, if the application should abort, else false. (default is false)
     * @return the new instance of the data replicator
     */
    public DataReplicator withFailOnInitFailure(boolean failOnInitFailure) {
        return new DataReplicator(this.uri,
                                  failOnInitFailure,
                                  this.cacheDir, 
                                  this.maxCacheTime, 
                                  this.appId,
                                  this.refreshPeriod);
    }
    
    
    /**
     * 
     * @param cacheDir  the cache dir (default is current working dir)
     * @return the new instance of the data replicator
     */
    public DataReplicator withCacheDir(File cacheDir) {
        return new DataReplicator(this.uri, 
                                  this.failOnInitFailure, 
                                  cacheDir, 
                                  this.maxCacheTime,
                                  this.appId,
                                  this.refreshPeriod);
    }
    

    /**
     * 
     * @param appID  the app identifier such as myApp/2.1 (default is datareplicator/1.0) 
     * @return the new instance of the data replicator
     */
    public DataReplicator withAppId(String appID) {
        return new DataReplicator(this.uri, 
                                  this.failOnInitFailure, 
                                  this.cacheDir, 
                                  this.maxCacheTime,
                                  appId,
                                  this.refreshPeriod);
    }
    
    
    
    /**
     * @param consumer  the consumer which will be called each time updated data is fetched  
     * @return the replication job
     */
    public ReplicationJob open(Consumer<byte[]> consumer) {
        return new ReplicatonJobImpl(uri, 
                                     failOnInitFailure, 
                                     cacheDir,
                                     maxCacheTime,
                                     appId,
                                     refreshPeriod,
                                     consumer);
    }
 
    
    
    
    
    
    
    private static final class ReplicatonJobImpl implements ReplicationJob {
        
        private final Datasource datasource; 
        private final FileCache fileCache;
        private final Consumer<byte[]> consumer;
        private final ScheduledExecutorService executor;
        
        
        public ReplicatonJobImpl(URI uri,
                                 boolean failOnInitFailure, 
                                 File cacheDir, 
                                 Duration maxCacheTime,
                                 String appId,
                                 Duration refreshPeriod, 
                                 Consumer<byte[]> consumer) {
            this.consumer = new CachingConsumer(consumer);
            this.fileCache = new FileCache(cacheDir, uri.toString(), maxCacheTime);
            

            // create proper data source 
            if (uri.getScheme().equalsIgnoreCase("classpath")) {
                this.datasource = new ClasspathDatasource(uri);
                
            } else if (uri.getScheme().equalsIgnoreCase("http") || uri.getScheme().equalsIgnoreCase("https")) {
                this.datasource = new HttpDatasource(uri, appId);
                
            } else if (uri.getScheme().equalsIgnoreCase("file")) {
                this.datasource = new FileDatasource(uri);
             
            } else {
                throw new RuntimeException("scheme of " + uri + " is not supported (supported: classpath, http, https)");
            }

            
            
            // load on startup
            try {
                load();
                
            } catch (RuntimeException rt) {
                
                if (failOnInitFailure) {
                    throw rt;
                } else {
                    // try to load from cache 
                    consumer.accept(fileCache.load());
                } 
            }
            
            
            // start scheduler
            this.executor = Executors.newScheduledThreadPool(0);
            executor.scheduleWithFixedDelay(() -> load(), refreshPeriod.toMillis(), refreshPeriod.toMillis(), TimeUnit.MILLISECONDS);
        }
    
        
        @Override
        public void close() {
            executor.shutdown();;
        }
        
        
        private void load() {
            try {
                final byte[] data = datasource.load();
                consumer.accept(data);
                fileCache.update(data); // data has been accepted by the consumer -> update cache
                
            } catch (RuntimeException rt) {
                LOG.warn("error occured by loading " + getEndpoint(), rt);
                throw rt;
            }
        }
        
        
        @Override
        public URI getEndpoint() {
            return datasource.getEndpoint();
        }
        
        @Override
        public Optional<Duration> getExpiredTimeSinceRefresh() {
            return datasource.getExpiredTimeSinceRefresh();
        }
    

        
        
        private static final class CachingConsumer implements Consumer<byte[]> {

            private final Consumer<byte[]> consumer;
            private final AtomicReference<Long> lastMd5 = new AtomicReference<>(Hashing.md5().newHasher().hash().asLong());
            
            public CachingConsumer(Consumer<byte[]> consumer) {
                this.consumer = consumer;
            }
            
            @Override
            public void accept(byte[] binary) {
                
                final long md5 = Hashing.md5().newHasher().putBytes(binary).hash().asLong();
                if (md5 != lastMd5.get()) {
                    consumer.accept(binary);
                    lastMd5.set(md5);
                }
            }
        }
        
        
        private static abstract class Datasource {
            
            private final URI uri;
            private final AtomicReference<Optional<Instant>> lastRefresh = new AtomicReference<>(Optional.empty());
            
            
            public Datasource(URI uri) {
                this.uri = uri;
            }
            
            public URI getEndpoint() {
                return uri;
            }
            
            
            public Optional<Duration> getExpiredTimeSinceRefresh() {
                return lastRefresh.get().map(time -> Duration.between(time, Instant.now()));
            }
            
            public byte[] load() {
                final byte[] data = onLoad();
                lastRefresh.set(Optional.of(Instant.now()));
                
                return data;
            }
                
            public abstract byte[] onLoad();
        }
        
      
       
        private static class ClasspathDatasource extends Datasource {
            
            public ClasspathDatasource(URI uri) {
                super(uri);
            }
            

            @Override
            public Optional<Duration> getExpiredTimeSinceRefresh() {
                return Optional.of(Duration.ZERO); 
            }

            
            @Override
            public byte[] onLoad() {
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
            public byte[] onLoad() {
                
                File file = new File(getEndpoint().getPath());
                if (file.exists()) {
                    
                    try {
                        return Files.toByteArray(file);
                    } catch (IOException ioe) {
                        throw new DatasourceException(ioe);
                    }
                    
                } else {
                    throw new RuntimeException("file  " + file.getAbsolutePath() + " not found");
                }
            }
        }

        
                
        
        private static class HttpDatasource extends Datasource {
            
            private final String appId;
            
            public HttpDatasource(URI uri, String appId) {
                super(uri);
                this.appId = appId;
            }
            
            @Override
            public Optional<Duration> getExpiredTimeSinceRefresh() {
                return Optional.empty();
            }
            
            @Override
            public byte[] onLoad() throws DatasourceException {
                
                InputStream is = null;
                try {
                    final URLConnection con = getEndpoint().toURL().openConnection();
                    con.setRequestProperty("X-APP", appId);

                    is = con.getInputStream();
                    return ByteStreams.toByteArray(is);
                    
                } catch (IOException ioe) {
                    throw new DatasourceException(ioe);
                } finally {
                    Closeables.closeQuietly(is);
                }
            }
            
        }
        
        
        
        private static final class FileCache extends Datasource {
            
            private final String cacheFileName;
            private final Duration maxCacheTime;
            
            
            
            public FileCache(File cacheDir, String name, Duration maxCacheTime) {
                this(new File(new File(cacheDir, "datareplicator"), name.replaceAll("[,:/]", "_")), maxCacheTime);
            }
                
            
            public FileCache(File cacheFile, Duration maxCacheTime) {
                super(cacheFile.toURI());

                this.maxCacheTime = maxCacheTime;
                
                cacheFile.getParentFile().mkdirs();
                cacheFileName = cacheFile.getAbsolutePath();
            }
            
            

            public void update(byte[] data) {
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

                        final File cacheFile = new File(cacheFileName);
                        if (cacheFile.exists()) {
                            cacheFile.delete();
                        }
                        tempFile.renameTo(cacheFile);
                        
                    } finally {
                        Closeables.close(os, true);
                    }
                    
                    
                }catch (IOException ioe) {
                    LOG.warn("updating cache file " + cacheFileName + " failed", ioe);
                }
            }
             

            @Override
            public Optional<Duration> getExpiredTimeSinceRefresh() {
                final File cacheFile = new File(cacheFileName);
                if (cacheFile.exists()) {
                    return Optional.of(Duration.between(Instant.ofEpochMilli(cacheFile.lastModified()), Instant.now()));
                } else  {
                    return Optional.empty();
                }
            }
   
            
            @Override
            public byte[] onLoad() throws DatasourceException {

                final Duration age = getExpiredTimeSinceRefresh().orElseThrow(() -> new DatasourceException("no cache entry exists"));
                if (maxCacheTime.minus(age).isNegative()) {
                    throw new DatasourceException("cache entry is eppired. Age is " + age.toDays() + " days");
                }
                
                final File cacheFile = new File(cacheFileName);
                if (cacheFile.exists()) {
                    FileInputStream is = null;
                    try {
                        is = new FileInputStream(cacheFile);
                        return ByteStreams.toByteArray(is);
                    } catch (IOException ioe) {
                        throw new DatasourceException("loading cachefile " + cacheFileName + " failed", ioe);
                    } finally {
                        Closeables.closeQuietly(is);
                    }
                } else {
                    throw new DatasourceException("no cache entry exists");
                }
            }
        }        
    }
}
