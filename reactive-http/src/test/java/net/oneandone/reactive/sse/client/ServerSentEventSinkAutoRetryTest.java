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
import net.oneandone.reactive.WebContainer;
import net.oneandone.reactive.sse.ServerSentEvent;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


public class ServerSentEventSinkAutoRetryTest {
    
        
    
    @Ignore
    @Test
    public void testWithRetry() throws Exception {
        WebContainer server = new WebContainer("/ssetest");
        server.start();
            
        URI uri = URI.create(server.getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());
        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>(1, 1000);
        new ClientSsePublisher(uri).subscribe(consumer); 

        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.buffer(1000)
                                                                 .subscribe(new ClientSseSubscriber(uri).autoId(true).withRetries(3));
        
        
        
        reactiveSink.accept(ServerSentEvent.newEvent().data("test1"));
        reactiveSink.accept(ServerSentEvent.newEvent().data("test21212"));
        reactiveSink.accept(ServerSentEvent.newEvent().data("test312123123123123"));
        
        
        sleep(300);
        Assert.assertEquals(3, consumer.getNumReceived());
        
        
        reactiveSink.accept(ServerSentEvent.newEvent().data("posion pill"));
        sleep(300);

        
        
        reactiveSink.accept(ServerSentEvent.newEvent().data("ttt"));
        
        sleep(300);
        
        Assert.assertEquals(4, consumer.getNumReceived());
        reactiveSink.accept(ServerSentEvent.newEvent().data("ttt4"));
        
        sleep(300);
        
        Assert.assertEquals(5, consumer.getNumReceived());
    }
    

    
    
    @Test
    public void testWithoutRetry() throws Exception {
        WebContainer server = new WebContainer("/ssetest");
        server.start();
            
        URI uri = URI.create(server.getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());
        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>(1, 1000);
        new ClientSsePublisher(uri).subscribe(consumer); 

        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.buffer(1000)
                                                                 .subscribe(new ClientSseSubscriber(uri).autoId(true));
        reactiveSink.accept(ServerSentEvent.newEvent().data("test1"));
        reactiveSink.accept(ServerSentEvent.newEvent().data("test21212"));
        reactiveSink.accept(ServerSentEvent.newEvent().data("test312123123123123"));
        
        
        sleep(300);
        Assert.assertEquals(3, consumer.getNumReceived());
        
        
        reactiveSink.accept(ServerSentEvent.newEvent().data("posion pill"));
        sleep(300);
        
   
        try {
            reactiveSink.accept(ServerSentEvent.newEvent().data("ttt4"));
            Assert.fail("IllegalStateException expected");
        } catch (IllegalStateException expected) {  }
        
    }
    

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) {
            
        }
    }
}