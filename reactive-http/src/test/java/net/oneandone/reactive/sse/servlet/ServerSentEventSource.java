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
package net.oneandone.reactive.sse.servlet;



import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Queue;

import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.ServerSentEventParser;


import com.google.common.collect.Lists;



public class ServerSentEventSource {

    private final HttpURLConnection httpCon;
    private final InputStream is;
    
    private final Queue<ServerSentEvent> bufferedEvents = Lists.newLinkedList();
    private final ServerSentEventParser parser = new ServerSentEventParser();
    private final byte buf[] = new byte[1024];
    private int len = -1;

    
    
    public ServerSentEventSource(URI url) throws IOException {
        httpCon = (HttpURLConnection) url.toURL().openConnection();
        httpCon.setDoOutput(false);
        httpCon.setDoInput(true);
        httpCon.setRequestProperty("Accept", "text/event-stream");
        httpCon.setRequestMethod("GET");
        is = httpCon.getInputStream();
    }
    
    
    public void close() {
        httpCon.disconnect();
    }
    
    
    
    public ServerSentEvent next() throws IOException {
        
        // no events buffered        
        if (bufferedEvents.isEmpty()) {
            
            // read network
            while (bufferedEvents.isEmpty()) {
                len = is.read(buf);
                parser.parse(ByteBuffer.wrap(buf, 0, len)).forEach(event -> bufferedEvents.add(event)); 
            }    
        }
          
        return bufferedEvents.poll();
    }
}
