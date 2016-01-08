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

import java.io.Closeable;
import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;

import com.google.common.base.Preconditions;


/**
 * Represents the replication job
 *
 */
public interface ReplicationJob extends Closeable {
    
    public static final boolean DEFAULT_FAIL_ON_INITFAILURE = false;
    public static final File DEFAULT_CACHEDIR = new File(".");
    public static final Duration DEFAULT_MAX_CACHETIME = Duration.ofDays(4);
    public static final Duration DEFAULT_REFRESHPERIOD = Duration.ofSeconds(60);

    
    /**
     * @return  the resource end point
     */
    URI getEndpoint();
    
    /**
     * @return the expired time since the last successfully refresh
     */
    Optional<Duration> getExpiredTimeSinceRefreshSuccess();

    /**
     * @return the expired time since the last erroneous refresh
     */
    Optional<Duration> getExpiredTimeSinceRefreshError();
    
    /**
     * @return the max cache time
     */
    Duration getMaxCacheTime();
    
    /**
     * @return the refresh period
     */
    Duration getRefreshPeriod();
    
    /**
     * terminates the replication job 
     */
    void close();
    
    

    
    /**
     * @param uri  the source uri. Supported schemes are <i>file</i>, <i>http</i>, <i>https</i> and <i>classpath</i> 
     *             (e.g. file:/C:/dev/workspace/reactive2/reactive-kafka-example/src/main/resources/schemas.zip, 
     *              classpath:schemas/schemas.zip, http://myserver/schemas.zip)  
     */
    static ReplicationJobBuilder source(final String uri) {
        Preconditions.checkNotNull(uri);
        return source(URI.create(uri));
    }
    
    /**
     * @param uri  the source uri. Supported schemes are <i>file</i>, <i>http</i>, <i>https</i> and <i>classpath</i> 
     *             (e.g. file:/C:/dev/workspace/reactive2/reactive-kafka-example/src/main/resources/schemas.zip, 
     *              classpath:schemas/schemas.zip, http://myserver/schemas.zip)  
     */
    static ReplicationJobBuilder source(final URI uri) {
        Preconditions.checkNotNull(uri);
        return new ReplicationJobBuilderImpl(uri, 
                                             DEFAULT_FAIL_ON_INITFAILURE, 
                                             DEFAULT_CACHEDIR, 
                                             DEFAULT_MAX_CACHETIME, 
                                             DEFAULT_REFRESHPERIOD, 
                                             null); 
    }
}