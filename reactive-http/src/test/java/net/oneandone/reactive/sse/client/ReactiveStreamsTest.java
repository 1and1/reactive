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
import net.oneandone.reactive.ReactiveSource;
import net.oneandone.reactive.TestServletbasedTest;
import net.oneandone.reactive.sse.ServerSentEvent;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;


public class ReactiveStreamsTest extends TestServletbasedTest  {
    
 
    
    @Test
    public void testSimple() throws Exception {        
    
        URI uri = URI.create(getServer().getBaseUrl() + "/simpletest/channel/" + UUID.randomUUID().toString());
        
        
        
        Publisher<ServerSentEvent> eventPublisher = new ClientSseSource(uri);   
        ReactiveSource<ServerSentEvent> reactiveSource = ReactiveSource.subscribe(eventPublisher);
        
        Subscriber<ServerSentEvent> eventSubscriber = new ClientSseSink(uri);
        ReactiveSink<ServerSentEvent> reactiveSink = ReactiveSink.publish(eventSubscriber);
  
        

        reactiveSink.write(ServerSentEvent.newEvent().data("testeventSinkSimple1"));
        reactiveSink.write(ServerSentEvent.newEvent().data("testeventSinkSimple21212"));
        reactiveSink.write(ServerSentEvent.newEvent().data("testeventSinkSimple312123123123123"));
        

        Assert.assertEquals("testeventSinkSimple1", reactiveSource.read().getData().get());
        Assert.assertEquals("testeventSinkSimple21212", reactiveSource.read().getData().get());
        Assert.assertEquals("testeventSinkSimple312123123123123", reactiveSource.read().getData().get());
        
        reactiveSource.close();        
        reactiveSink.shutdown();
    }
 }