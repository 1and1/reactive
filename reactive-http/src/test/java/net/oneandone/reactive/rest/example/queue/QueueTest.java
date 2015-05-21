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
package net.oneandone.reactive.rest.example.queue;





import java.net.URI;
import java.util.UUID;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import net.oneandone.reactive.ReactiveSource;
import net.oneandone.reactive.WebContainer;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.client.ClientSseSource;

import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;




@Ignore
public class QueueTest {
    
    private Client client;
    private WebContainer server;
    
   
    @Before
    public void before() throws Exception {
        server = new WebContainer("/queuemanager");
        server.start();
        
        client = new ResteasyClientBuilder().connectionPoolSize(5).build(); 
    }
   
    
    @After
    public void after() throws Exception {
        server.stop();
        client.close();
    }
    
    
    @Test
    public void testClassic() throws Exception {
        String id1 = UUID.randomUUID().toString();
        String id2 = UUID.randomUUID().toString();
        String id3 = UUID.randomUUID().toString();

        
        
        
        // add an event
        Response response = client.target(server.getBaseUrl() + "/queues/customerchange/messages/" + id1 + "?ttl=50")
                                  .request()
                                  .put(Entity.text("added1"));
        
        // add an event
        response = client.target(server.getBaseUrl() + "/queues/customerchange/messages/" + id2)
                         .request()
                         .put(Entity.text("added2"));
        
        // add an event
        response = client.target(server.getBaseUrl() + "/queues/customerchange/messages/" + id3 + "?ttl=5")
                         .request()
                         .put(Entity.text("added3"));
        
        
        sleep(500);

        ReactiveSource<ServerSentEvent> source = new ClientSseSource(URI.create(server.getBaseUrl() + "/queues/customerchange/messages/")).open();

        
        sleep(500);

        
     // add an event
        response = client.target(server.getBaseUrl() + "/queues/customerchange/messages/" + id3 + "?ttl=5")
                         .request()
                         .put(Entity.text("added4"));

        
        System.out.println(source.read().getData().get());
        System.out.println(source.read().getData().get());
        System.out.println(source.read().getData().get());
        System.out.println(source.read().getData().get());
        System.out.println("end"); 
        
    }
    
    
    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) { }
    }
}