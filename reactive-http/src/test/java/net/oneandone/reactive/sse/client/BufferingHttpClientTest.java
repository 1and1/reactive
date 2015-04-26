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
import java.util.Iterator;
import java.util.UUID;

import net.oneandone.reactive.AbstractProcessor;
import net.oneandone.reactive.WebContainer;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.client.Producer.Emitter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class BufferingHttpClientTest {
    
    
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

        TestSubscriber consumer = new TestSubscriber(1, 3);
        new ClientSsePublisher(uri).subscribe(consumer); 
        
        
        

        Producer<ServerSentEvent> producer = new Producer<>();
        
        
        AbstractProcessor<ServerSentEvent> streamBuffer = new AbstractProcessor<>(10);
        producer.subscribe(streamBuffer);
        
        producer.subscribe(new ClientSseSubscriber(uri, true));
        
        Emitter<ServerSentEvent> emitter = producer.getEmitterAsync().get();
        emitter.publish(ServerSentEvent.newEvent().data("test1"));
        emitter.publish(ServerSentEvent.newEvent().data("test21212"));
        emitter.publish(ServerSentEvent.newEvent().data("test312123123123123"));
        
        
        
        Iterator<ServerSentEvent> sseIt = consumer.getEventsAsync().get().iterator();
        Assert.assertEquals("test1", sseIt.next().getData());
        Assert.assertEquals("test21212", sseIt.next().getData());
        Assert.assertEquals("test312123123123123", sseIt.next().getData());
        
        producer.close();
    }
}