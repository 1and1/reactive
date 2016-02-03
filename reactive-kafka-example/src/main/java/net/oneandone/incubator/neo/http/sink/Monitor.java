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

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;


import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;



final class Monitor implements HttpSink.Metrics {
    private final Set<TransientSubmission> runningQuery = Collections.newSetFromMap(new WeakHashMap<TransientSubmission, Boolean>());
    private final MetricRegistry metrics = new MetricRegistry();
    private final Counter success = metrics.counter("success");
    private final Counter retries = metrics.counter("retries");
    private final Counter discarded = metrics.counter("discarded");
    private final Counter rejected = metrics.counter("rejected");

    public void incSuccess() {
        success.inc();
    }
    
    @Override
    public Counter getNumSuccess() {
        return success;
    }
    
    public void incRetry() {
        retries.inc();
    }

    @Override
    public Counter getNumRetries() {
        return retries;
    }
    
    public void incReject() {
        rejected.inc();
    }

    @Override
    public Counter getNumRejected() {
        return rejected;
    }

    public void incDiscard() {
        discarded.inc();
    }
    
    @Override
    public Counter getNumDiscarded() {
        return discarded;
    }
    
    @Override
    public int getNumPending() {
        return getAll().size();
    }
    
    public void onAcquired(TransientSubmission query) {
        synchronized (this) {
            runningQuery.add(query);
        }
    }
    
    public void onReleased(TransientSubmission query) {
        synchronized (this) {
            runningQuery.remove(query);
        }
    }

    public ImmutableSet<TransientSubmission> getAll() {
        ImmutableSet<TransientSubmission> runnings;
        synchronized (this) {
            runnings = ImmutableSet.copyOf(runningQuery);
        }
        return runnings;
    }
    
    @Override
    public String toString() {
        return new StringBuilder().append("panding=" + getNumPending())
                                  .append("success=" + getNumSuccess().getCount())
                                  .append("retries=" + getNumRetries().getCount())
                                  .append("discarded=" + getNumDiscarded().getCount())
                                  .append("rejected=" + getNumRejected().getCount())
                                  .toString();
    }
}