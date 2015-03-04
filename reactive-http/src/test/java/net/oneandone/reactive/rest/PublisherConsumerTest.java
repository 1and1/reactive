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
package net.oneandone.reactive.rest;


import javax.ws.rs.ClientErrorException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import net.oneandone.reactive.WebContainer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;





public class PublisherConsumerTest {
    
    private WebContainer server;
    private Client client;
    
    
    
    @Before
    public void before() throws Exception {
        server = new WebContainer("/resttest");
        server.start();
        
        client = ClientBuilder.newClient();
    }
   
    
    @After
    public void after() throws Exception {
        client.close();
        server.stop();
    }

    
    
    
    
    @Test
    public void testPublisherFirst() throws Exception {

        String element = client.target(server.getBaseUrl() + "/Publisher/first/?num=1")
                               .request()
                               .get(String.class);
        Assert.assertEquals("1", element);
     
        
        
        element = client.target(server.getBaseUrl() + "/Publisher/first?num=2")
                        .request()
                        .get(String.class);
        Assert.assertEquals("1", element);
     
        
        
        Response response = client.target(server.getBaseUrl() + "/Publisher/first?num=0")
                                  .request()
                                  .get();
        Assert.assertEquals(204, response.getStatus());
        response.close();
     
        
        
        try {
            client.target(server.getBaseUrl() + "/Publisher/first?num=-1")
                  .request()
                  .get(String.class);
            Assert.fail("InternalServerErrorException expected");
        } catch (InternalServerErrorException expected) { }
    }
   
    
    
    @Test
    public void testPublisherSingle() throws Exception {

        String element = client.target(server.getBaseUrl() + "/Publisher/single/?num=1")
                               .request()
                               .get(String.class);
        Assert.assertEquals("1", element);
     
        
        try {
            client.target(server.getBaseUrl() + "/Publisher/single?num=2")
                  .request()
                  .get(String.class);
            Assert.fail("ClientErrorException expected");
        } catch (ClientErrorException expected) {
            Assert.assertEquals(409, expected.getResponse().getStatus());
        }
     
        
        
        Response response = client.target(server.getBaseUrl() + "/Publisher/single?num=0")
                                  .request()
                                  .get();
        Assert.assertEquals(404, response.getStatus());
        response.close();
        
        
        try {
            client.target(server.getBaseUrl() + "/Publisher/single?num=-1")
                  .request()
                  .get(String.class);
            Assert.fail("InternalServerErrorException expected");
        } catch (InternalServerErrorException expected) { }
    }

}