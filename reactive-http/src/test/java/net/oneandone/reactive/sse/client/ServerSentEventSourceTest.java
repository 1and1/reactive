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
import java.time.Duration;
import java.util.UUID;

import net.oneandone.reactive.ConnectException;
import net.oneandone.reactive.ReactiveSink;
import net.oneandone.reactive.ReactiveSource;
import net.oneandone.reactive.TestServletbasedTest;
import net.oneandone.reactive.sse.ServerSentEvent;

import org.junit.Assert;
import org.junit.Test;




public class ServerSentEventSourceTest extends TestServletbasedTest {
    
    /*
    public ServerSentEventSourceTest() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
    }
    */
    
    @Test
    public void testSimple() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());

        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(uri).open();    
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(uri).open();
        
        
        for (int i = 0; i < 10; i++) {
            reactiveSink.write(ServerSentEvent.newEvent().data("testsimple" + i));
        }
        
        Assert.assertEquals("testsimple0", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple1", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple2", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple3", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple4", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple5", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple6", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple7", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple8", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple9", reactiveSource.read().getData().get());
        
        reactiveSource.close();
        reactiveSink.shutdown();
    }
    
    

    @Test
    public void testIncomingBufferSize0() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());

        
        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(uri)
                                                                    .buffer(0)
                                                                    .open();
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(uri).open();
        
        
        for (int i = 0; i < 5; i++) {
            reactiveSink.write(ServerSentEvent.newEvent().data("testsimple" + i));
        }
        
        sleep(500); 
        Assert.assertTrue(reactiveSource.toString().contains("numPendingRequests=0 numBuffered=0  (subscription: [suspended]"));
        
        Assert.assertEquals("testsimple0", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple1", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple2", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple3", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple4", reactiveSource.read().getData().get());
        
        reactiveSource.close();
        reactiveSink.shutdown();
    }
    
    
    
    @Test
    public void testIncomingBufferSize3() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());

        
        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(uri).buffer(3).open();    
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(uri).open();        
        
        for (int i = 0; i < 5; i++) {
            reactiveSink.write(ServerSentEvent.newEvent().data("testsimple" + i));
        }
        
        sleep(500); 
        Assert.assertTrue(reactiveSource.toString().contains("numPendingRequests=0 numBuffered=0  (subscription: [suspended]"));
        
        Assert.assertEquals("testsimple0", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple1", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple2", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple3", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple4", reactiveSource.read().getData().get());
        
        reactiveSource.close();
        reactiveSink.shutdown();
    }
    
    
    @Test
    public void testIncomingBufferSizeDefault() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());

        
        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(uri).open();    
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(uri).open();
        
        
        for (int i = 0; i < 5; i++) {
            reactiveSink.write(ServerSentEvent.newEvent().data("testsimple" + i));
        }
        
        sleep(500); 
        Assert.assertTrue(reactiveSource.toString().contains("numPendingRequests=0 numBuffered=0"));
        Assert.assertTrue(reactiveSource.toString().contains("buffered events: 5"));
        
        Assert.assertEquals("testsimple0", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple1", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple2", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple3", reactiveSource.read().getData().get());
        Assert.assertEquals("testsimple4", reactiveSource.read().getData().get());
        
        reactiveSource.close();
        reactiveSink.shutdown();
    }
    
   
    

    
    @Test
    public void testInboundStartWithLastEventId() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());
        
        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(uri).open();    
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(uri).open();        

        
        reactiveSink.write(ServerSentEvent.newEvent().id("1").data("testStartWithLastEventId1"));
        reactiveSink.write(ServerSentEvent.newEvent().id("2").data("testStartWithLastEventId2"));
        reactiveSink.write(ServerSentEvent.newEvent().id("3").data("testStartWithLastEventId3"));
        
        Assert.assertEquals("testStartWithLastEventId1", reactiveSource.read().getData().get());
        Assert.assertEquals("testStartWithLastEventId2", reactiveSource.read().getData().get());
        Assert.assertEquals("testStartWithLastEventId3", reactiveSource.read().getData().get());
        
        reactiveSource.close();    
        
        
        
        reactiveSource = new ClientSseSource(uri).withLastEventId("1").open();
        Assert.assertEquals("testStartWithLastEventId2", reactiveSource.read().getData().get());
        Assert.assertEquals("testStartWithLastEventId3", reactiveSource.read().getData().get());
        
        reactiveSource.close();
        reactiveSink.shutdown();
    }
    
    
    
    

    @Test
    public void testInboundConnectionTerminated() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());
        
        
        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(uri).open();    
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(uri).open();
        
        
        // sendig data 
        reactiveSink.write(ServerSentEvent.newEvent().data("testInboundConnectionTerminated1"));
        reactiveSink.write(ServerSentEvent.newEvent().data("testInboundConnectionTerminated2"));
        
        Assert.assertEquals("testInboundConnectionTerminated1", reactiveSource.read().getData().get());
        Assert.assertEquals("testInboundConnectionTerminated2", reactiveSource.read().getData().get());

        
        // invalidate the connection
        reactiveSink.write(ServerSentEvent.newEvent().event("posion pill"));
        reactiveSink.shutdown();
        
        sleep(500);
        
        reactiveSink = new ClientSseSink(uri).open();

        reactiveSink.write(ServerSentEvent.newEvent().data("testInboundConnectionTerminated3"));
        Assert.assertEquals("testInboundConnectionTerminated3", reactiveSource.read().getData().get());
        
        
        reactiveSink.shutdown();
    }
    
    

    @Test
    public void testInboundConnectionServerDown() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());
        
        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(uri).open();    
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(uri).open();
        
        
        // sendig data 
        reactiveSink.write(ServerSentEvent.newEvent().data("testInboundConnectionServerDown1"));
        reactiveSink.write(ServerSentEvent.newEvent().data("testInboundConnectionServerDown2"));

        Assert.assertEquals("testInboundConnectionServerDown1", reactiveSource.read().getData().get());
        Assert.assertEquals("testInboundConnectionServerDown2", reactiveSource.read().getData().get());
        
        // invalidate the connection
        reactiveSink.write(ServerSentEvent.newEvent().event("knockout drops").data("1000"));
        reactiveSink.shutdown();

        
        sleep(4000);
        
        reactiveSink = new ClientSseSink(uri).open();
        
        reactiveSink.write(ServerSentEvent.newEvent().data("testInboundConnectionServerDown3"));
        Assert.assertEquals("testInboundConnectionServerDown3", reactiveSource.read().getData().get());
        
        reactiveSink.write(ServerSentEvent.newEvent().event("knockout drops").data("1000"));
        
        
        reactiveSink.shutdown();
    }
    

    @Test
    public void testRedirected() throws Exception {
        String id = UUID.randomUUID().toString();
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + id);
        URI redirectUri = URI.create(getServer().getBaseUrl() + "/simpletest/redirect/" + id + "/?num=3");
        
        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(redirectUri).open();    
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(uri).open();
        
        reactiveSink.write(ServerSentEvent.newEvent().data("test1"));
        reactiveSink.write(ServerSentEvent.newEvent().data("test2"));
        
        Assert.assertEquals("test1", reactiveSource.read().getData().get());
        Assert.assertEquals("test2", reactiveSource.read().getData().get());

        reactiveSource.close();
        reactiveSink.shutdown();        
    }
    

    @Test
    public void testMaxRedirectedExceeded() throws Exception {
        String id = UUID.randomUUID().toString();
        URI redirectUri = URI.create(getServer().getBaseUrl() + "/simpletest/redirect/" + id + "/?num=11");
        
        try {
            new ClientSseSource(redirectUri).open();   
            Assert.fail("ConnectException expected");
        } catch (ConnectException expected) { 
            Assert.assertTrue(expected.getMessage().contains("got unexpected 303 response"));
        }         
    }
    
    


    @Test
    public void testNotFoundError() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/notfound/");
        
        try {
            new ClientSseSource(uri).open();
            Assert.fail("ConnectException expected");
        } catch (ConnectException expected) { 
            Assert.assertTrue(expected.getMessage().contains("got unexpected 404 response"));
        }
    }

    

    @Test
    public void testNonexistingURI() throws Exception {
        URI uri = URI.create("http://145.43.42.4:7890/hostnotexist");
        
        try {
            new ClientSseSource(uri).connectionTimeout(Duration.ofMillis(250)).open();    
            Assert.fail("ConnectException expected");
        } catch (ConnectException expected) { 
            Assert.assertTrue(expected.getMessage().contains("connection timed out"));
        }
    }
    

    @Test
    public void testServerError() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/servererror/");
        
        try {
            new ClientSseSource(uri).open();    
            Assert.fail("ConnectException expected");
        } catch (ConnectException expected) { 
            Assert.assertTrue(expected.getMessage().contains("got unexpected 500 response"));
        }
    }
    
    
    @Test
    public void testIgnoreErrorOnConnect() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/servererror/");
        
        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(uri).failOnConnectError(false).open();    

        sleep(400);
        Assert.assertTrue(reactiveSource.toString().contains("(subscription: [not connected]"));
        
        reactiveSource.close();
        
        sleep(400);
        Assert.assertTrue(reactiveSource.toString().contains("subscription: [closed]"));
    }
}