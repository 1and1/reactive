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

import net.oneandone.reactive.ReactiveSink;
import net.oneandone.reactive.ReactiveSource;
import net.oneandone.reactive.WebContainer;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.client.ClientSseSource;
import net.oneandone.reactive.sse.client.ClientSseSink;

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


        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(url).open();    
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(url).open();
        sleep(500);  // wait for internal async connects
                
        
        reactiveSink.write(ServerSentEvent.newEvent()
                                          .id("1")
                                          .event("evt2")
                                          .data("data")
                                          .comment("comment")
                                          .retry(11));
        
        ServerSentEvent event = reactiveSource.read();
        Assert.assertEquals("1", event.getId().get());
        Assert.assertEquals("evt2", event.getEvent().get());
        Assert.assertEquals("data", event.getData().get());

        reactiveSource.close();
        reactiveSink.shutdown();
    }
    

    
    @Test
    public void testBulk() throws Exception {
        URI url = URI.create(server.getBaseUrl() + "/sse/channel/" + UUID.randomUUID().toString());

        
        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(url).open();    
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(url).open();
        sleep(500);  // wait for internal async connects
                
        
        System.out.println("sending small number of events");
        new Thread() {
            
            public void run() {
                for (int i = 0; i < 10; i++) {
                    reactiveSink.write(ServerSentEvent.newEvent().data("testBulk" + i + " ..............................................................................................................................................................................................................................................................................................................................."));
                }
            };
            
        }.start();

        Assert.assertTrue(reactiveSource.read().getData().get().startsWith("testBulk0"));
        Assert.assertTrue(reactiveSource.read().getData().get().startsWith("testBulk1"));
        Assert.assertTrue(reactiveSource.read().getData().get().startsWith("testBulk2"));
        Assert.assertTrue(reactiveSource.read().getData().get().startsWith("testBulk3"));

        System.out.println("pausing");
        sleep(500);
        
        
        System.out.println("receiving more");
        Assert.assertTrue(reactiveSource.read().getData().get().startsWith("testBulk4"));
        Assert.assertTrue(reactiveSource.read().getData().get().startsWith("testBulk5"));

        
        
        System.out.println("sending large number of events");
        new Thread() {
            
            public void run() {
                for (int i = 0; i < 1000000; i++) {
                    reactiveSink.write(ServerSentEvent.newEvent().data("testBulk" + i + " ..............................................................................................................................................................................................................................................................................................................................."));
                }
            };
            
        }.start();


        sleep(1000);
        
        reactiveSource.close();
        reactiveSink.shutdown();
    }
    
    

    protected void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) {
            
        }
    }
}