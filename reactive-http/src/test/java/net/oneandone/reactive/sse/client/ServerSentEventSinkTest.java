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

import net.oneandone.reactive.ReactiveSink;
import net.oneandone.reactive.TestSubscriber;
import net.oneandone.reactive.sse.ServerSentEvent;

import org.junit.Assert;
import org.junit.Test;


public class ServerSentEventSinkTest extends TestServletbasedTest  {
    
    
    @Test
    public void testSimple() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());
        
        TestSubscriber<ServerSentEvent> consumer = new TestSubscriber<>();
        new ClientSsePublisher(uri).subscribe(consumer); 
        consumer.waitForSubscribedAsync();


        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.buffer(1000)
                                                                 .subscribe(new ClientSseSubscriber(uri).autoId(true));
        
        sleep(500);  // wait for internal async connects
        
        
        reactiveSink.accept(ServerSentEvent.newEvent().data("test1"));
        reactiveSink.accept(ServerSentEvent.newEvent().data("test21212"));
        reactiveSink.accept(ServerSentEvent.newEvent().data("test312123123123123"));
        
        
        
        Iterator<ServerSentEvent> sseIt = consumer.getEventsAsync(3).get().iterator();
        Assert.assertEquals("test1", sseIt.next().getData().get());
        Assert.assertEquals("test21212", sseIt.next().getData().get());
        Assert.assertEquals("test312123123123123", sseIt.next().getData().get());
        
        reactiveSink.shutdown();
        
        System.out.println("closed");
    }
}