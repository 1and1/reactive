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



import java.net.URI;
import java.util.UUID;

import net.oneandone.reactive.WebContainer;
import net.oneandone.reactive.sse.ServerSentEvent;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;





public class ReactiveSseServletTest {
    
    private WebContainer server;
    
   
    @Before
    public void before() throws Exception {
        server = new WebContainer("/ssetest");
        server.start();
    }
   
    
    @After
    public void after() throws Exception {
        server.stop();
    }

    
    
    @Test
    public void testSimple() throws Exception {
        URI url = URI.create(server.getBaseUrl() + "/sse/channel/" + UUID.randomUUID().toString());

        ServerSentEventSink sseOut = new ServerSentEventSink(url);
        sseOut.write(ServerSentEvent.newEvent()
                             .id("1")
                             .event("evt2")
                             .data("data")
                             .comment("comment")
                             .retry(11));
        

        
        ServerSentEventSource sseIn = new ServerSentEventSource(url);
        
        ServerSentEvent event = sseIn.next();
        Assert.assertEquals("1", event.getId().get());
        Assert.assertEquals("evt2", event.getEvent().get());
        Assert.assertEquals("data", event.getData().get());

        
        sseOut.close();
        sseIn.close();
    }
    

    
    @Test
    public void testBulk() throws Exception {
        URI url = URI.create(server.getBaseUrl() + "/sse/channel/" + UUID.randomUUID().toString());

        final ServerSentEventSink sseOut = new ServerSentEventSink(url);
        
        System.out.println("sending small number of events");
        new Thread() {
            
            public void run() {
                for (int i = 0; i < 10; i++) {
                    sseOut.write(ServerSentEvent.newEvent().data("test" + i + " ..............................................................................................................................................................................................................................................................................................................................."));
                }
            };
            
        }.start();
                

        System.out.println("receiving");

        ServerSentEventSource sseIn = new ServerSentEventSource(url);
        
        Assert.assertTrue(sseIn.next().getData().get().startsWith("test0"));
        Assert.assertTrue(sseIn.next().getData().get().startsWith("test1"));
        Assert.assertTrue(sseIn.next().getData().get().startsWith("test2"));
        Assert.assertTrue(sseIn.next().getData().get().startsWith("test3"));

        System.out.println("pausing");
        pause(500);
        
        
        System.out.println("receiving more");
        Assert.assertTrue(sseIn.next().getData().get().startsWith("test4"));
        Assert.assertTrue(sseIn.next().getData().get().startsWith("test5"));

        
        
        System.out.println("sending large number of events");
        new Thread() {
            
            public void run() {
                for (int i = 0; i < 1000000; i++) {
                    sseOut.write(ServerSentEvent.newEvent().data("test" + i + " ..............................................................................................................................................................................................................................................................................................................................."));
                }
            };
            
        }.start();


        System.out.println("pausing");
        pause(1000);

        
        System.out.println("closing");
        sseOut.close();
        sseIn.close();
    }
    
    
    private void pause(int millis) {
      try {
            Thread.sleep(millis);
      } catch (InterruptedException ignore) { }
    }
}