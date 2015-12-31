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
package net.oneandone.commons.incubator.neo.http.client;


import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.core.MultivaluedMap;



public class AddAppHeaderClientFilter implements ClientRequestFilter {
    
    private final String headername; 
    private final String appName;
    private final String appVersion;
    
    public AddAppHeaderClientFilter(final String headername, final String appName, final String appVersion) {
        this.headername = headername;
        this.appName = appName;
        this.appVersion = appVersion;

    }
    
    
    @Override
    public void filter(ClientRequestContext requestContext) {
        MultivaluedMap<String, Object> headers = requestContext.getHeaders();
        if (!headers.containsKey(headername)) {
            headers.add(headername, appName + "/" + appVersion);
        }
    }
}