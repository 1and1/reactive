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
package net.oneandone.reactive.sse.client;


import java.time.Duration;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;


abstract class SseEndpoint { 
    
    protected static final int DEFAULT_NUM_FOILLOW_REDIRECTS = 9;
    
    
    
    protected static final class RetrySequence {
        private ImmutableMap<Duration, Duration> delayMap;
        
        public RetrySequence(int... delaysMillis) {
            Map<Duration, Duration> map = Maps.newHashMap();
            
            for (int i = 0; i < delaysMillis.length; i++) {
                map.put(Duration.ofMillis(delaysMillis[i]), Duration.ofMillis( (delaysMillis.length > (i+1)) ? delaysMillis[i+1] : delaysMillis[i]) );
            }
            
            delayMap = ImmutableMap.copyOf(map);
        }
        
        public Duration nextDelay(Duration previous) {
            return (delayMap.get(previous) == null) ? previous : delayMap.get(previous);
        }
    }
}  