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
package net.oneandone.reactive;



import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.ws.rs.client.Client;

import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.junit.After;
import org.junit.Before;


public abstract class TestServletbasedTest {
    
    private Client client; 
    private WebContainer server;
    
   
    @Before
    public void before() throws Exception {
        server = new WebContainer("/ssetest");
        server.start();
        
        client = new ResteasyClientBuilder().socketTimeout(60, TimeUnit.SECONDS)
                                            .connectionPoolSize(10)
                                            .build();
    }
   
    
    @After
    public void after() throws Exception {
        client.close();
        server.stop();
    }

    
    protected Client getClient() {
        return client;
    }
    
    protected WebContainer getServer() {
        return server;
    }
    
 

    protected void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) {
            
        }
    }
    
    
    protected void waitUtil(Supplier<Boolean> condition, long maxSec) {
        
        long maxWaittimeMillis = maxSec * 1000; 
        long sleeptimeMillis = 100;
        for (int i = 0; i < (maxWaittimeMillis / sleeptimeMillis); i++) {
            if (condition.get()) {
                return;
            } else {
                sleep(sleeptimeMillis);
            }
        }
    }
}