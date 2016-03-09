/*
 * Copyright 1&1 Internet AG, htt;ps://github.com/1and1/
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

import java.net.URI;
import javax.ws.rs.client.Entity;
import com.google.common.collect.ImmutableSet;
import net.oneandone.incubator.neo.http.sink.HttpSink.Method;


/**
 * Represent the submission process 
 */
public interface Submission {
    
    /**
     * submission state
     */
    public enum State { PENDING, COMPLETED, DISCARDED } 
    
    /**
     * @return the state 
     */
    State getState();
    

    /**
     * @return the id
     */
    String getId();

	/**
	 * @return the method
	 */
	Method getMethod();
    
    /**
     * @return the target
     */
	URI getTarget();

	/**
	 * @return the entity
	 */
	Entity<?> getEntity();

	/**
	 * @return the reject status list
	 */
	ImmutableSet<Integer> getRejectStatusList();
}