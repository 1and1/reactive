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





import java.util.UUID;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import net.oneandone.reactive.WebContainer;
import net.oneandone.reactive.sse.ServerSentEvent;

import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;





public class QueueTest {
    
    private Client client;
    private WebContainer server;
    
   
    @Before
    public void before() throws Exception {
        server = new WebContainer("/queues");
        server.start();
        
        client = new ResteasyClientBuilder().connectionPoolSize(5).build(); 
    }
   
    
    @After
    public void after() throws Exception {
        server.stop();
        client.close();
    }

    
    
    @Ignore
    @Test
    public void testClassic() throws Exception {
        
        Response response = client.target(server.getBasePath() + "/customerchange")
                                  .request()
                                  .get();        
        String id1 = response.getHeaderString("x-ui-msg-next-id");
        
         
        
        // add an event
        response = client.target(server.getBasePath() + "/customerchange/" + id1)
                         .request()
                         .put(Entity.text("added1"));
        String id2 = response.getHeaderString("x-ui-msg-next-id");
        
        // add an event
        response = client.target(server.getBasePath() + "/customerchange/" + id2)
                         .request()
                         .put(Entity.text("added2"));
        String id3 = response.getHeaderString("x-ui-msg-next-id");        
        
        // add an event
        response = client.target(server.getBasePath() + "/customerchange/" + id3)
                         .request()
                         .put(Entity.text("added3"));
        String id4 = response.getHeaderString("x-ui-msg-next-id");        
        
              

        // get an event
        String data = client.target(server.getBasePath() + "/customerchange/event?lastEventId=" + id2)
                            .request()
                            .get(String.class);
      
        
        
    }
}