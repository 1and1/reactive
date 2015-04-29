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
import java.util.UUID;

import net.oneandone.reactive.ReactiveSink;
import net.oneandone.reactive.TestSubscriber;
import net.oneandone.reactive.sse.ServerSentEvent;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;




public class ServerSentEventSourceTest extends TestServletbasedTest {
    
    
    
    @Test
    public void testInboundSuspending() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());

        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSsePublisher(uri).subscribe(consumer); 


        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.buffer(1000)
                                                                 .subscribe(new ClientSseSubscriber(uri));
        
        sleep(500);  // wait for internal async connects
        
        
        for (int i = 0; i < 10; i++) {
            reactiveSink.accept(ServerSentEvent.newEvent().data("test" + i));
        }
        
        consumer.getEventsAsync(10).get();

        
        consumer.suspend();

        
        for (int i = 0; i < 20; i++) {
            reactiveSink.accept(ServerSentEvent.newEvent().data("test2" + i));
        }
        
        
        consumer.getEventsAsync(11).get();
        
        Assert.assertTrue(consumer.toString().contains("[suspended]"));
        Assert.assertEquals(11, consumer.getNumReceived());


        
        consumer.resume();


        consumer.getEventsAsync(30).get();
        Assert.assertFalse(consumer.toString().contains("[suspended]"));

        

        for (int i = 0; i < 70; i++) {
            reactiveSink.accept(ServerSentEvent.newEvent().data("test3" + i));
        }
        
        
        consumer.getEventsAsync(100).get();
        
        reactiveSink.shutdown();
    }
    
    

    
    @Test
    public void testInboundStartWithLastEventId() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());
        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSsePublisher(uri).subscribe(consumer); 


        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.buffer(1000)
                                                                 .subscribe(new ClientSseSubscriber(uri).autoId(true));

        
        sleep(500);  // wait for interna lasync connects
        
        reactiveSink.accept(ServerSentEvent.newEvent().id("1").data("test1"));
        reactiveSink.accept(ServerSentEvent.newEvent().id("2").data("test2"));
        reactiveSink.accept(ServerSentEvent.newEvent().id("3").data("test3"));
        
        consumer.getEventsAsync(3).get();
        
        
        TestSubscriber<ServerSentEvent> consumer2 = new TestSubscriber<>();
        new ClientSsePublisher(uri).withLastEventId("1")
                                   .subscribe(consumer2); 
        
        ImmutableList<ServerSentEvent> events = consumer2.getEventsAsync(2).get();
        Assert.assertEquals("2", events.get(0).getId().get());
        Assert.assertEquals("3", events.get(1).getId().get());
        
        
        
        reactiveSink.shutdown();
    }
    
    
    
    

    @Test
    public void testInboundConnectionTerminated() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());
        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSsePublisher(uri).subscribe(consumer); 


        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.buffer(1000)
                                                                 .subscribe(new ClientSseSubscriber(uri));
        
        sleep(500);  // wait for internal async connects
        
        
        // sendig data 
        reactiveSink.accept(ServerSentEvent.newEvent().data("test1"));
        reactiveSink.accept(ServerSentEvent.newEvent().data("test2"));
        consumer.getEventsAsync(2).get();

        
        // invalidate the connection
        reactiveSink.accept(ServerSentEvent.newEvent().data("posion pill"));
        reactiveSink.shutdown();
        
        sleep(500);
        
        reactiveSink = ReactiveSink.buffer(1000)
                                   .subscribe(new ClientSseSubscriber(uri));
        sleep(500);  // wait for internal async connects

        
        
        reactiveSink.accept(ServerSentEvent.newEvent().data("test3"));
        
        ImmutableList<ServerSentEvent> events = consumer.getEventsAsync(3).get();
        Assert.assertEquals("test1", events.get(0).getData().get());
        Assert.assertEquals("test2", events.get(1).getData().get());
        Assert.assertEquals("test3", events.get(2).getData().get());
        
        
        reactiveSink.shutdown();
    }
    
    

    @Test
    public void testInboundConnectionServerDown() throws Exception {

    }
    

    @Test
    public void testRedirectConnecet() throws Exception {
        
    }
}