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
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;

import net.oneandone.reactive.sse.ServerSentEvent;


public class ServerSentEventSink {

    private final HttpURLConnection httpCon;
    private final OutputStream os;

    
    public ServerSentEventSink(URI url) throws IOException {
        httpCon = (HttpURLConnection) url.toURL().openConnection();
        httpCon.setDoOutput(true);
        httpCon.setDoInput(true);
        httpCon.setRequestProperty("Content-Type", "text/event-stream");
        httpCon.setChunkedStreamingMode(10);

        httpCon.setRequestMethod("POST");
        os = httpCon.getOutputStream();
    }
    
    
    public void write(ServerSentEvent event) {
        try {
            os.write(event.toString().getBytes("UTF-8"));
            os.flush();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }


    public void close() {
        httpCon.disconnect();
    }
}
