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

import net.oneandone.reactive.WebContainer;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.client.Producer.Emitter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;




public class HttpClientMaxReceiveBufferSizeExceddedTest {
    
    
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
        URI uri = URI.create(server.getBaseUrl() + "/sse/channel/" + UUID.randomUUID().toString());

        
        TestSubscriber consumer = new TestSubscriber(10, 100);
        new ClientSsePublisher(uri).subscribe(consumer); 


        Producer<ServerSentEvent> producer = new Producer<>();
        producer.subscribe(new ClientSseSubscriber(uri, true));
        
        Emitter<ServerSentEvent> emitter = producer.getEmitterAsync().get();
        
        consumer.suspend();
        
        for (int i = 0; i < 30; i++) {
            emitter.publish(ServerSentEvent.newEvent().data("test" + i));
        }
        
        
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) { }
        
        
        Assert.assertEquals(10, consumer.getNumReceived());
        Assert.assertTrue(consumer.toString().contains("[suspended]"));

        consumer.resume();

        

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) { }

        
        Assert.assertEquals(30, consumer.getNumReceived());
        Assert.assertFalse(consumer.toString().contains("[suspended]"));

        

        for (int i = 0; i < 70; i++) {
            emitter.publish(ServerSentEvent.newEvent().data("test" + i));
        }
        
        
        
        
        Assert.assertEquals(100,  consumer.getEventsAsync().get().size());
        
        producer.close();
    }
}