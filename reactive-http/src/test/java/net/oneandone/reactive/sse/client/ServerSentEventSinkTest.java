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
import java.util.Map;
import java.util.UUID;

import net.oneandone.reactive.ConnectException;
import net.oneandone.reactive.ReactiveSink;
import net.oneandone.reactive.ReactiveSource;
import net.oneandone.reactive.sse.ServerSentEvent;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Maps;


public class ServerSentEventSinkTest extends TestServletbasedTest  {
    
    private static final String LARGE_TEXT = "DSDGSRHEDHGSFDFADFWSFSADFQEWRTSFDGASFDFSADFASTFRWAERTWSGSDFSDFSDGFSRGTWRTWERGSFGSDFsdfaser" +
                                             "gfsdfgsdfgsagadfgsafgsgsgasfgasfdgasdfgdsfgaerzqtehdbycbnsfthastrhdfadfbyfxbadfgaehgatedhd" +
                                             "affdaffbdfadfhadthadhdatrhdadfsrzsfietzurthadthatehzutrzhadthadfadgtghtarhzqethadthadthadg" +
                                             "gfsdfgsdfgsagadfgsafgsgsgasfgasfdgasdfgdsfgaerzqtehdbycbnsfthastrhdfadfbyfxbadfgaehgatedhd" +
                                             "affdaffbdfadfhadthadhdatrhdadfsrzsfietzurthadthatehzutrzhadthadfadgtghtarhzqethadthadthadg" +
                                             "gfsdfgsdfgsagadfgsafgsgsgasfgasfdgasdfgdsfgaerzqtehdbycbnsfthastrhdfadfbyfxbadfgaehgatedhd" +
                                             "affdaffbdfadfhadthadhdatrhdadfsrzsfietzurthadthatehzutrzhadthadfadgtghtarhzqethadthadthadg" +
                                             "gfsdfgsdfgsagadfgsafgsgsgasfgasfdgasdfgdsfgaerzqtehdbycbnsfthastrhdfadfbyfxbadfgaehgatedhd" +
                                             "affdaffbdfadfhadthadhdatrhdadfsrzsfietzurthadthatehzutrzhadthadfadgtghtarhzqethadthadthadg";

    
    public ServerSentEventSinkTest() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
    }
  
    
    @Test
    public void testSimple() throws Exception {        
    
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());
        
        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(uri).open();    
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(uri).open();
  

        reactiveSink.write(ServerSentEvent.newEvent().data("testeventSinkSimple1"));
        reactiveSink.write(ServerSentEvent.newEvent().data("testeventSinkSimple21212"));
        reactiveSink.write(ServerSentEvent.newEvent().data("testeventSinkSimple312123123123123"));
        

        Assert.assertEquals("testeventSinkSimple1", reactiveSource.read().getData().get());
        Assert.assertEquals("testeventSinkSimple21212", reactiveSource.read().getData().get());
        Assert.assertEquals("testeventSinkSimple312123123123123", reactiveSource.read().getData().get());
        
        reactiveSource.close();        
        reactiveSink.shutdown();
    }
    

    @Ignore
    @Test
    public void testWriteBufferOverflow() throws Exception {        
    
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());
        
        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(uri).open();    
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(uri).buffer(Integer.MAX_VALUE).open();
  
        reactiveSink.write(ServerSentEvent.newEvent().event("soporific").data("500"));
        
        
        int numLoops = 1000;
        
        for (int i = 0; i < numLoops; i++) {
            reactiveSink.write(ServerSentEvent.newEvent().data("test" + i + LARGE_TEXT));
        }
        

        Map<String, ServerSentEvent> result = Maps.newHashMap();
        for (int i = 0; i < numLoops; i++) {
            ServerSentEvent event = reactiveSource.read();
            System.out.println(event.getId().get());
            result.put(event.getId().get(), event);
        }


        
        reactiveSource.close();        
        reactiveSink.shutdown();
    }
    




    @Test
    public void testConnectionTerminated() throws Exception {
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
        
        sleep(500);
        
        reactiveSink.write(ServerSentEvent.newEvent().data("testInboundConnectionTerminated3"));
        Assert.assertEquals("testInboundConnectionTerminated3", reactiveSource.read().getData().get());
        
        
        reactiveSink.shutdown();
    }

    
    
    @Test
    public void testIgnoreErrorOnConnect() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/servererror/");
        
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(uri).failOnConnectError(false).open();

        sleep(400);
        Assert.assertTrue(reactiveSink.toString().contains("(subscription: [not connected]"));
        
        reactiveSink.close();
        
        sleep(400);
        Assert.assertTrue(reactiveSink.toString().contains("subscription: [closed]"));
    }
    

    

    @Test
    public void testRedirected() throws Exception {
        String id = UUID.randomUUID().toString();
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + id);
        URI redirectUri = URI.create(getServer().getBaseUrl() + "/simpletest/redirect/" + id + "/?num=3");
        
        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(uri).open();    
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(redirectUri).open();
        
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
            new ClientSseSink(redirectUri).open();
            Assert.fail("ConnectException expected");
        } catch (ConnectException expected) {  }         
    }
    

    @Test
    public void testNotFoundError() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/notfound/");
        
        try {
            new ClientSseSink(uri).open();
            Assert.fail("ConnectException expected");
        } catch (ConnectException expected) {  }
    }

    
    

    @Test
    public void testNonexistingURI() throws Exception {
        URI uri = URI.create("http://145.43.42.4:7890/hostnotexist");
        
        try {
            new ClientSseSink(uri).connectionTimeout(Duration.ofMillis(250)).open();    
            Assert.fail("ConnectException expected");
        } catch (ConnectException expected) {   }
    }
    
    @Test
    public void testServerError() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/servererror/");
        
        try {
            new ClientSseSink(uri).open();    
            Assert.fail("ConnectException expected");
        } catch (ConnectException expected) {  }
    }
}