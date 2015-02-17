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




import java.net.URI;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.client.ClientBuilder;

import net.oneandone.reactive.WebContainer;
import net.oneandone.reactive.rest.client.RxClient;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;





public class ResultConsumerTest {
    
    private WebContainer server;
    
    
    @Before
    public void before() throws Exception {
        server = new WebContainer("/resttest");
        server.start();
    }
   
    
    @After
    public void after() throws Exception {
        server.stop();
    }

    
    
    @Test
    public void testSimple() throws Exception {
        RxClient client = new RxClient(ClientBuilder.newClient());
        
        String resp = client.target(URI.create(server.getBaseUrl() + "/MyResource/45"))
                            .request()
                            .rx()
                            .get(String.class)
                            .get();
        Assert.assertEquals("45", resp);
        
        
        try {
            resp = client.target(URI.create(server.getBaseUrl() + "/MyResource/666"))
                         .request()
                         .get(String.class);
            Assert.fail("ServerErrorException expected");
        } catch (ServerErrorException expected) { }
   

        try {
            resp = client.target(URI.create(server.getBaseUrl() + "/MyResource/666"))
                         .request()
                         .rx()
                         .get(String.class)
                         .get();
            Assert.fail("ExecutionException expected");
        } catch (ExecutionException expected) {
            Assert.assertTrue(expected.getCause() instanceof InternalServerErrorException);
        }
        
        
        
        resp = client.target(URI.create(server.getBaseUrl() + "/MyResource/45"))
                     .request()
                     .rx()
                     .delete(String.class)
                     .get();
        Assert.assertNull(resp);
        
        
        
        
        client.target(URI.create(server.getBaseUrl() + "/MyResource/45"))
              .request()
              .rx()
              .get(String.class)
              .thenAccept(data -> System.out.println(data));
        
        client.close();
    }
}