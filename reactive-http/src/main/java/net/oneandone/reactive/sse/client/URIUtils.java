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

import java.net.URI;



class URIUtils {
    
    private URIUtils() { }
    
    
    public static String getScheme(URI uri) {
       return (uri.getScheme() == null) ? "http" : uri.getScheme();
    }
    
    public static int getPort(URI uri) {
        int port = uri.getPort();
        if (port == -1) {
            if ("http".equalsIgnoreCase(getScheme(uri))) {
                return 80;
            } else if ("https".equalsIgnoreCase(getScheme(uri))) {
                return 443;
            }
        } 
            
        return port;
    }
}