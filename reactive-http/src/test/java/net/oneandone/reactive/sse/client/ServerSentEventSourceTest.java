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
import java.util.concurrent.ExecutionException;

import net.oneandone.reactive.ReactiveSink;
import net.oneandone.reactive.TestSubscriber;
import net.oneandone.reactive.sse.ServerSentEvent;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;




public class ServerSentEventSourceTest extends TestServletbasedTest {
    
    
    @Test
    public void testSimple() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());

        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSseSource(uri).subscribe(consumer);
        consumer.waitForSubscribedAsync().get();


        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.buffer(1000)
                                                                 .subscribe(new ClientSseSink(uri));
        
        sleep(500);  // wait for internal async connects
        
        
        for (int i = 0; i < 10; i++) {
            reactiveSink.accept(ServerSentEvent.newEvent().data("testsimple" + i));
        }
        
        consumer.getEventsAsync(10).get();
        
        reactiveSink.shutdown();
    }
    
    
    
    
    @Test
    public void testInboundSuspending() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());

        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSseSource(uri).subscribe(consumer);
        consumer.waitForSubscribedAsync().get();


        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.buffer(1000)
                                                                 .subscribe(new ClientSseSink(uri));
        
        sleep(500);  // wait for internal async connects
        
        
        for (int i = 0; i < 10; i++) {
            reactiveSink.accept(ServerSentEvent.newEvent().data("testsuspending" + i));
        }
        
        consumer.getEventsAsync(10).get();

        
        consumer.suspend();

        
        for (int i = 0; i < 20; i++) {
            reactiveSink.accept(ServerSentEvent.newEvent().data("testsuspending2" + i));
        }
        
        
        consumer.getEventsAsync(11).get();
        
        Assert.assertTrue(consumer.toString().contains("[suspended]"));
        Assert.assertEquals(11, consumer.getNumReceived());


        
        consumer.resume();


        consumer.getEventsAsync(30).get();
        Assert.assertFalse(consumer.toString().contains("[suspended]"));

        

        for (int i = 0; i < 70; i++) {
            reactiveSink.accept(ServerSentEvent.newEvent().data("testsuspending3" + i));
        }
        
        
        consumer.getEventsAsync(100).get();
        
        reactiveSink.shutdown();
    }
    
    

    
    @Test
    public void testInboundStartWithLastEventId() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());
        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSseSource(uri).subscribe(consumer); 
        consumer.waitForSubscribedAsync().get();


        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.buffer(1000)
                                                                 .subscribe(new ClientSseSink(uri).autoId(true));

        
        sleep(500);  // wait for internal async connects
        
        reactiveSink.accept(ServerSentEvent.newEvent().id("1").data("testStartWithLastEventId1"));
        reactiveSink.accept(ServerSentEvent.newEvent().id("2").data("testStartWithLastEventId2"));
        reactiveSink.accept(ServerSentEvent.newEvent().id("3").data("testStartWithLastEventId3"));
        
        consumer.getEventsAsync(3).get();
        
        
        TestSubscriber<ServerSentEvent> consumer2 = new TestSubscriber<>();
        new ClientSseSource(uri).withLastEventId("1")
                                   .subscribe(consumer2); 
        consumer2.waitForSubscribedAsync();

        
        ImmutableList<ServerSentEvent> events = consumer2.getEventsAsync(2).get();
        Assert.assertEquals("2", events.get(0).getId().get());
        Assert.assertEquals("3", events.get(1).getId().get());
        
        
        
        reactiveSink.shutdown();
    }
    
    
    
    

    @Test
    public void testInboundConnectionTerminated() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());
        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSseSource(uri).subscribe(consumer); 
        consumer.waitForSubscribedAsync().get();


        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.buffer(1000)
                                                                 .subscribe(new ClientSseSink(uri));
        
        sleep(500);  // wait for internal async connects
        
        
        // sendig data 
        reactiveSink.accept(ServerSentEvent.newEvent().data("testInboundConnectionTerminated1"));
        reactiveSink.accept(ServerSentEvent.newEvent().data("testInboundConnectionTerminated2"));
        consumer.getEventsAsync(2).get();

        
        // invalidate the connection
        reactiveSink.accept(ServerSentEvent.newEvent().event("posion pill"));
        reactiveSink.shutdown();
        
        sleep(500);
        
        reactiveSink = ReactiveSink.buffer(1000)
                                   .subscribe(new ClientSseSink(uri));
        sleep(500);  // wait for internal async connects

        
        
        reactiveSink.accept(ServerSentEvent.newEvent().data("testInboundConnectionTerminated3"));
        
        ImmutableList<ServerSentEvent> events = consumer.getEventsAsync(3).get();
        Assert.assertEquals("testInboundConnectionTerminated1", events.get(0).getData().get());
        Assert.assertEquals("testInboundConnectionTerminated2", events.get(1).getData().get());
        Assert.assertEquals("testInboundConnectionTerminated3", events.get(2).getData().get());
        
        
        reactiveSink.shutdown();
    }
    
    

    @Test
    public void testInboundConnectionServerDown() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());
        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSseSource(uri).subscribe(consumer); 
        consumer.waitForSubscribedAsync().get();


        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.buffer(1000)
                                                                 .subscribe(new ClientSseSink(uri));
        
        sleep(500);  // wait for internal async connects
        
        
        // sendig data 
        reactiveSink.accept(ServerSentEvent.newEvent().data("testInboundConnectionServerDown1"));
        reactiveSink.accept(ServerSentEvent.newEvent().data("testInboundConnectionServerDown2"));
        consumer.getEventsAsync(2).get();

        
        // invalidate the connection
        reactiveSink.accept(ServerSentEvent.newEvent().event("knockout drops").data("1000"));
        reactiveSink.shutdown();

        
        sleep(4000);
        
        reactiveSink = ReactiveSink.buffer(1000)
                                   .subscribe(new ClientSseSink(uri));
        sleep(500);  // wait for internal async connects

        
        
        reactiveSink.accept(ServerSentEvent.newEvent().data("testInboundConnectionServerDown3"));
        
        ImmutableList<ServerSentEvent> events = consumer.getEventsAsync(3).get();
        Assert.assertEquals("testInboundConnectionServerDown1", events.get(0).getData().get());
        Assert.assertEquals("testInboundConnectionServerDown2", events.get(1).getData().get());
        Assert.assertEquals("testInboundConnectionServerDown3", events.get(2).getData().get());
        
        
        reactiveSink.shutdown();
    }
    

    @Ignore
    @Test
    public void testRedirected() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/redirect/" + UUID.randomUUID().toString());
        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSseSource(uri).subscribe(consumer);
        consumer.waitForSubscribedAsync().get();


        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.buffer(1000)
                                                                 .subscribe(new ClientSseSink(uri));
        
        sleep(500);  // wait for internal async connects
        
        
        reactiveSink.accept(ServerSentEvent.newEvent().data("test1"));
        reactiveSink.accept(ServerSentEvent.newEvent().data("test2"));
        
        ImmutableList<ServerSentEvent> events = consumer.getEventsAsync(2).get();
        Assert.assertEquals("test1", events.get(0).getData().get());
        Assert.assertEquals("test2", events.get(1).getData().get());

        
        reactiveSink.shutdown();        
    }
    
    

    @Test
    public void testNotFoundError() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/notfound/");
        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSseSource(uri).subscribe(consumer);
        try {
            consumer.waitForSubscribedAsync().get();
            Assert.fail("ExecutionException expected");
        } catch (ExecutionException expected) {
            Assert.assertTrue(expected.getMessage().contains("404 Not Found response received"));
        }
    }


    @Test
    public void testServerError() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/servererror/");
        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSseSource(uri).subscribe(consumer);
        
        try {
            consumer.waitForSubscribedAsync().get();
            Assert.fail("ExecutionException expected");
        } catch (ExecutionException expected) { 
            Assert.assertTrue(expected.getMessage().contains("500 Internal Server Error response received"));
        }
    }
    
    
    @Test
    public void testIgnoreErrorOnConnect() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/servererror/");
        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSseSource(uri).failOnConnectError(false).subscribe(consumer);
        consumer.waitForSubscribedAsync();

        sleep(400);
        Assert.assertTrue(consumer.toString().contains("subscription [not connected]"));
        
        consumer.close();
        
        sleep(400);
        Assert.assertTrue(consumer.toString().contains("subscription [closed]"));
    }
}