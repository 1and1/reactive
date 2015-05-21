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
package net.oneandone.reactive.rest.client;



import java.net.URI;

import javax.ws.rs.client.Client;

import net.oneandone.reactive.ReactiveSink;
import net.oneandone.reactive.TestServletbasedTest;

import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;


public class RxClientSubscriberTest extends TestServletbasedTest  {
    
    
    @Test
    public void testMaxInFlight() throws Exception {        
        Client client = new ResteasyClientBuilder().connectionPoolSize(5).build(); 
        
        URI uri = URI.create(getServer().getBaseUrl() + "/sink/?pauseMillis=700");
        System.out.println(uri);
        
        
        Subscriber<String> subscriber = new RxClientSubscriber<String>(client, uri).maxInFlight(3);
        ReactiveSink<String> reactiveSink = ReactiveSink.buffersize(0).publish(subscriber);
        
        reactiveSink.write("1_test");

        Assert.assertTrue(reactiveSink.isWriteable());
        reactiveSink.write("2_test");
        
        Assert.assertTrue(reactiveSink.isWriteable());
        reactiveSink.write("3_test");
        sleep(200);
        
        Assert.assertFalse(reactiveSink.isWriteable());
        try {
            reactiveSink.write("4_test");
            Assert.fail("IllegalStateException expected");
        } catch (IllegalStateException expected) {}
        
        
        client.close();
        reactiveSink.shutdown();
    }
}