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
import java.net.URI;
import java.time.Duration;
import java.util.Optional;


/**
 * Represents the replication job
 *
 */
public interface ReplicationJob extends Closeable {
    
    
    /**
     * @return  the resource endpoint
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
     * terminates the replication job 
     */
    void close();
}
