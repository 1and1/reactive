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


import net.oneandone.reactive.ReactiveSink;
import net.oneandone.reactive.TestServletbasedTest;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;


public class RxClientSubscriberTest extends TestServletbasedTest  {
    
    /*
    public RxClientSubscriberTest() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
    }
    */
    
    @Test
    public void testMaxInFlight() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/sink/?pauseMillis=700");
        System.out.println(uri);
        
        
        Subscriber<String> subscriber = new RxClientSubscriber<String>(getClient(), uri).maxInFlight(3);
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
        
        reactiveSink.shutdown();
    }
    
    
    @Test
    public void testServerErrorOnStartIgnore() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/sink/servererror");
        
        Subscriber<String> subscriber = new RxClientSubscriber<String>(getClient(), uri).failOnInitialError(false);
        ReactiveSink<String> sink = ReactiveSink.publish(subscriber);
        
        sink.write("test");
        
        sleep(200);
        Assert.assertTrue(sink.isOpen());
    }    
    
    

    @Test
    public void testServerErrorOnStart() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/sink/servererror");
        
        Subscriber<String> subscriber = new RxClientSubscriber<String>(getClient(), uri);
        ReactiveSink<String> sink = ReactiveSink.publish(subscriber);
        
        sink.write("test");
        
        sleep(200);
        Assert.assertFalse(sink.isOpen());
    }    
    

    @Test
    public void testServerErrorOnStartNotFound() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/sink/notfound");
        
        Subscriber<String> subscriber = new RxClientSubscriber<String>(getClient(), uri);
        ReactiveSink<String> sink = ReactiveSink.publish(subscriber);
        
        sink.write("test");
        
        sleep(200);
        Assert.assertFalse(sink.isOpen());
    }    
    
    
    
    @Test
    public void testAutoretry() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/sink/");
        
        Subscriber<String> subscriber = new RxClientSubscriber<String>(getClient(), uri);
        ReactiveSink<String> sink = ReactiveSink.publish(subscriber);
        
        for (int i = 0; i < 10; i++) {
            sink.write("unreliable");
        }
        
        sleep(500);
        
        waitUtil(() -> subscriber.toString().contains("runningRetries: 0"), 5);
    }
    
    
    @Test
    public void testNoAutoretry() throws Exception {
        URI uri = URI.create(getServer().getBaseUrl() + "/sink/");
        
        Subscriber<String> subscriber = new RxClientSubscriber<String>(getClient(), uri).autoRetry(false);
        ReactiveSink<String> sink = ReactiveSink.publish(subscriber);
        sink.write("test1");
        sink.write("test2");
        
        sink.write("posion pill");
        sleep(500);

        Assert.assertFalse(sink.isOpen());
        
        try {
            sink.write("test");
            Assert.fail("IllegalStateException Expected");
        } catch (IllegalStateException expected) {  }
    }
}