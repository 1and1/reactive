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



import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.net.Socket;

import net.oneandone.reactive.WebContainer;
import net.oneandone.reactive.sse.ServerSentEvent;

import org.junit.Assert;
import org.junit.Test;


public class CancelTest {
    
        
    
    @Test
    public void testServerInitatedCancel() throws Exception {
        
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");
        
        
        WebContainer server = new WebContainer("/ssetest");
        server.start();
            
        
        Socket s = new Socket("localhost", server.getLocalPort());
        
        OutputStream os = s.getOutputStream();
        InputStream is = s.getInputStream();
        
        
        
        // send header
        String header = "POST " + server.getBasePath() + "/simpletest/channel/4455 HTTP/1.1\r\n" +
                        "Host: localhost:" + server.getLocalPort() +"\r\n" +
                        "User-Agent: me\r\n" +
                        "Content-Length: 100000\r\n" +
                        "\r\n";
        os.write(header.getBytes("ISO-8859-1"));
        os.flush();

        
        // send first event
        String event = ServerSentEvent.newEvent().data("first").toWire();
        os.write(event.getBytes("UTF-8"));
        os.flush();
        sleep(300);

        
        // send posion pill event
        event = ServerSentEvent.newEvent().event("posion pill").toWire();
        os.write(event.getBytes("UTF-8"));
        os.flush();
        sleep(300);
        

        LineNumberReader lnr = new LineNumberReader(new InputStreamReader(is));
        String line = lnr.readLine();
        Assert.assertTrue(line.contains("HTTP/1.1 200"));
   
        s.close();
    }
    

    
    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) {
            
        }
    }
}