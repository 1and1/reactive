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
import net.oneandone.reactive.TestSubscriber;
import net.oneandone.reactive.WebContainer;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.client.ClientSsePublisher;
import net.oneandone.reactive.sse.client.ClientSseSubscriber;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;





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

        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSsePublisher(url).subscribe(consumer); 
        consumer.waitForSubscribedAsync();


        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.buffer(1000)
                                                                 .subscribe(new ClientSseSubscriber(url).autoId(true));
        
        sleep(500);  // wait for internal async connects
                
        
        reactiveSink.accept(ServerSentEvent.newEvent()
                                           .id("1")
                                           .event("evt2")
                                           .data("data")
                                           .comment("comment")
                                           .retry(11));
        
        ServerSentEvent event = consumer.getEventsAsync(1).get().get(0);
        Assert.assertEquals("1", event.getId().get());
        Assert.assertEquals("evt2", event.getEvent().get());
        Assert.assertEquals("data", event.getData().get());

        consumer.close();
        reactiveSink.shutdown();
    }
    

    
    @Test
    public void testBulk() throws Exception {
        URI url = URI.create(server.getBaseUrl() + "/sse/channel/" + UUID.randomUUID().toString());

        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSsePublisher(url).subscribe(consumer); 
        consumer.waitForSubscribedAsync();


        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.buffer(1000)
                                                                 .subscribe(new ClientSseSubscriber(url).autoId(true));
        
        sleep(500);  // wait for internal async connects
                
        
        System.out.println("sending small number of events");
        new Thread() {
            
            public void run() {
                for (int i = 0; i < 10; i++) {
                    reactiveSink.accept(ServerSentEvent.newEvent().data("test" + i + " ..............................................................................................................................................................................................................................................................................................................................."));
                }
            };
            
        }.start();
                

        System.out.println("receiving");

        ImmutableList<ServerSentEvent> events = consumer.getEventsAsync(4).get();
        Assert.assertTrue(events.get(0).getData().get().startsWith("test0"));
        Assert.assertTrue(events.get(1).getData().get().startsWith("test1"));
        Assert.assertTrue(events.get(2).getData().get().startsWith("test2"));
        Assert.assertTrue(events.get(3).getData().get().startsWith("test3"));

        System.out.println("pausing");
        sleep(500);
        
        
        System.out.println("receiving more");
        events = consumer.getEventsAsync(6).get();
        Assert.assertTrue(events.get(4).getData().get().startsWith("test4"));
        Assert.assertTrue(events.get(5).getData().get().startsWith("test5"));

        
        
        System.out.println("sending large number of events");
        new Thread() {
            
            public void run() {
                for (int i = 0; i < 1000000; i++) {
                    reactiveSink.accept(ServerSentEvent.newEvent().data("test" + i + " ..............................................................................................................................................................................................................................................................................................................................."));
                }
            };
            
        }.start();


        System.out.println("pausing");
        sleep(1000);

        
        System.out.println("closing");
        consumer.close();
        reactiveSink.shutdown();
    }
    
    

    protected void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) {
            
        }
    }
}