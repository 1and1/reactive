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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;




public class DataReplicator {
    
    private static final Logger LOG = LoggerFactory.getLogger(DataReplicator.class);
    
    private final URI uri;
    private final boolean failOnInitFailure;
    private final Duration refreshPeriod;
    private final File cacheDir;
    private final String appId;
    private final Duration maxCacheTime;
    
    
    
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

    
    public DataReplicator withRefreshPeriod(Duration refreshPeriod) {
        return new DataReplicator(this.uri, 
                                  this.failOnInitFailure, 
                                  this.cacheDir, 
                                  this.maxCacheTime, 
                                  this.appId,
                                  refreshPeriod);
    }
    
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
     * @param failOnInitFailure true, if the application should abort, else false.
     */
    public DataReplicator withFailOnInitFailure(boolean failOnInitFailure) {
        return new DataReplicator(this.uri,
                                  failOnInitFailure,
                                  this.cacheDir, 
                                  this.maxCacheTime, 
                                  this.appId,
                                  this.refreshPeriod);
    }
    
    public DataReplicator withCacheDir(File cacheDir) {
        return new DataReplicator(this.uri, 
                                  this.failOnInitFailure, 
                                  cacheDir, 
                                  this.maxCacheTime,
                                  this.appId,
                                  this.refreshPeriod);
    }
    

    public DataReplicator withAppId(String appID) {
        return new DataReplicator(this.uri, 
                                  this.failOnInitFailure, 
                                  this.cacheDir, 
                                  this.maxCacheTime,
                                  appId,
                                  this.refreshPeriod);
    }
    
    
    
    public ReplicationJob open(Consumer<byte[]> consumer) {
        return new ReplicatonJobImpl(uri, failOnInitFailure, cacheDir, maxCacheTime, refreshPeriod, consumer);
    }
 
    
    
    
    
    private static final class ReplicatonJobImpl implements ReplicationJob {
        
        private final Datasource datasource; 
        private final FileCache fileCache;
        private final Consumer<byte[]> consumer;
        
        
        public ReplicatonJobImpl(URI uri,
                                 boolean failOnInitFailure, 
                                 File cacheDir, 
                                 Duration maxCacheTime,
                                 Duration reloadPeriod, 
                                 Consumer<byte[]> consumer) {
            this.consumer = new CachingConsumer(consumer);
            this.fileCache = new FileCache(cacheDir, uri.toString(), maxCacheTime);
            
            
            if (uri.getScheme().equals("classpath")) {
                this.datasource = new ClasspathDatasource(uri);
            } else {
                throw new RuntimeException("scheme of " + uri + " is not supported (supported: classpath)");
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
        }

        
        private void load() throws DatasourceException {
            byte[] data = datasource.load();
            consumer.accept(data);
            
            // data has been accepted by the consumer -> update cache
            fileCache.update(data);
        }
        
        
        @Override
        public URI getEndpoint() {
            return datasource.getEndpoint();
        }
        
        @Override
        public Optional<Duration> getExpiredTimeSinceRefresh() {
            return datasource.getExpiredTimeSinceRefresh();
        }
        
        @Override
        public void close() {
            datasource.close();
        }
        

        
        
        private static final class CachingConsumer implements Consumer<byte[]> {

            private final Consumer<byte[]> consumer;
            private final AtomicReference<HashCode> lastMd5 = new AtomicReference<HashCode>(Hashing.md5().newHasher().hash());
            
            public CachingConsumer(Consumer<byte[]> consumer) {
                this.consumer = consumer;
            }
            
            @Override
            public void accept(byte[] binary) {
                HashCode hc = Hashing.md5().newHasher().putBytes(binary).hash();
                
                if (!hc.equals(lastMd5)) {
                    consumer.accept(binary);
                    lastMd5.set(hc);
                }
            }
        }
        
        
        private static abstract class Datasource implements Closeable {
            
            private final URI uri;
            
            public Datasource(URI uri) {
                this.uri = uri;
            }
            
            public URI getEndpoint() {
                return uri;
            }
            
            @Override
            public void close() { }
            
            public abstract Optional<Duration> getExpiredTimeSinceRefresh();

            public abstract byte[] load() throws DatasourceException;
        }
        
      
        private static class ClasspathDatasource extends Datasource {
            
            public ClasspathDatasource(URI uri) {
                super(uri);
            }
            
            @Override
            public Optional<Duration> getExpiredTimeSinceRefresh() {
                return Optional.empty();
            }
            
            @Override
            public byte[] load() throws DatasourceException {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                if (classLoader == null) {
                    classLoader = getClass().getClassLoader();
                }
                
                URL classpathUri = classLoader.getResource(getEndpoint().getRawSchemeSpecificPart());           
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
                    File tempFile = new File(cacheFileName + ".temp");
                    if (tempFile.exists()) {
                        tempFile.delete();
                    }
                    
                    FileOutputStream os = null;
                    try {
                        os = new FileOutputStream(tempFile);
                        os.write(data);
                        os.close();

                        File cacheFile = new File(cacheFileName);
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
                File cacheFile = new File(cacheFileName);
                if (cacheFile.exists()) {
                    return Optional.of(Duration.between(Instant.ofEpochMilli(cacheFile.lastModified()), Instant.now()));
                } else  {
                    return Optional.empty();
                }
            }
   
            
            @Override
            public byte[] load() throws DatasourceException {

                Duration age = getExpiredTimeSinceRefresh().orElseThrow(() -> new DatasourceException("no cache entry exists"));
                if (maxCacheTime.minus(age).isNegative()) {
                    throw new DatasourceException("cache entry is eppired. Age is " + age.toDays() + " days");
                }
                
                File cacheFile = new File(cacheFileName);
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
