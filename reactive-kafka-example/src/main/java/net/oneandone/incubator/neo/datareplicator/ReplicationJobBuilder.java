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


import java.io.File;
import java.time.Duration;
import java.util.function.Consumer;

import javax.ws.rs.client.Client;

import com.google.common.collect.ImmutableList;




/**
 * The data replicator replicates data periodically from uri base resources. Data will be cached locally for availability reasons.
 * Each time updated data is fetched the registered consumer will be called. <br>
 * 
 * Typically the data replicator is used for configuration data, definition files and template data  
 * 
 * <pre>
 *  private static class MyClass implements Closeable {
 *      private final ReplicationJob replicationJob;
 *      
 *      public MyClass() {
 *          this.replicationJob = ReplicationJob.source(myUri)
 *                                              .withCacheDir(myCacheDir)
 *                                              .startConsumingTextList(this::onListReload);
 *      }
 *      
 *      void onListReload(ImmutableList<String> list) {
 *          //... parse list. If an parsing error occurs, a RuntimeException will be thrown
 *      }
 *
 *      // ...
 *      
 *      @Override
 *      public void close() {
 *          replicationJob.close();
 *      }
 *  }
 * <pre>
 *
 */
public interface ReplicationJobBuilder {    


    /**
     * @param refreshPeriod   the refresh period. The period should be as high as no unnecessary 
     *                        extra load on the resource server is generated. Furthermore it
     *                        should be as low as data is fresh enough. (default is 60 sec) 
     * @return the new instance of the data replicator
     */
    ReplicationJobBuilder withRefreshPeriod(final Duration refreshPeriod);
    
    /**
     * 
     * @param maxCacheTime  the max cache time. The max time data is cached. This means it is highly 
     *                      probable that a successfully refresh will be performed within this time 
     *                      period (even though serious incidents occurs). Furthermore the age of the 
     *                      data is acceptable for the consumer (default is {@link ReplicationJobBuilder#DEFAULT_MAX_CACHETIME})
     * @return the new instance of the data replicator
     */
    ReplicationJobBuilder withMaxCacheTime(final Duration maxCacheTime);
    
    /**
     * Sets the value whether the application should terminate the start-up process when the data (source and local copy) 
     * are not available.<p>
     * 
     * If failOnInitFailure==true then consumer method immediately aborts with a RuntimeException if the configured
     * source cannot be fetched.
     * <p>
     * If failOnInitFailure==false and the source is unreachable: If a cached file exists and not 
     * expired ({@link #withMaxCacheTime(Duration)}}), this file will be used. 
     * 
     * @param failOnInitFailure true, if the application should be aborted, else false. (default is {@link ReplicationJobBuilder#DEFAULT_FAIL_ON_INITFAILURE})
     * @return the new instance of the data replicator
     */
    ReplicationJobBuilder withFailOnInitFailure(final boolean failOnInitFailure);
    
    /**
     * 
     * @param cacheDir  the cache dir (default is {@link ReplicationJobBuilder#DEFAULT_CACHEDIR})
     * @return the new instance of the data replicator
     */
    ReplicationJobBuilder withCacheDir(final File cacheDir);
    
    /**
     * @param client the client to use
     * @return the new instance of the data replicator
     */
    ReplicationJobBuilder withClient(final Client client);
    
    /**
     * @param consumer  the binary data consumer which will be called each time updated data is fetched. If a 
     *                  parsing error occurs, the data consumer will throw a RuntimeException  
     * @return the replication job
     */
    ReplicationJob startConsumingBinary(final Consumer<byte[]> consumer);

    /**
     * @param consumer  the (UTF-8 encoded) text consumer which will be called each time updated data is fetched. If a 
     *                  parsing error occurs, the data consumer will throw a RuntimeException    
     * @return the replication job
     */
    ReplicationJob startConsumingText(final Consumer<String> consumer);
    
    /**
     * @param consumer  the (UTF-8 encoded, line break separated, trimmed) text list consumer which will be called each
     *                  time updated data is fetched. If a parsing error occurs, the data consumer will throw a RuntimeException    
     * @return the replication job
     */
    ReplicationJob startConsumingTextList(final Consumer<ImmutableList<String>> consumer);
}