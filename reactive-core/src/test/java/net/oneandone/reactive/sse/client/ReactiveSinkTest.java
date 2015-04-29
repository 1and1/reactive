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



import net.oneandone.reactive.ReactiveSink;

import org.junit.Assert;
import org.junit.Test;


public class ReactiveSinkTest {
    
    
    @Test
    public void testInsuffientBufferSize() throws Exception {
        TestSubscriber<String> consumer = new TestSubscriber<>(1);
        consumer.suspend();
        
        
        ReactiveSink<String> sink = ReactiveSink.buffer(1)
                                                .subscribe(consumer);
        
        sink.accept("1");
        sleep(300);
        Assert.assertEquals(0, sink.getUnprocessedElements().size());
        Assert.assertEquals(1, consumer.getNumReceived());
        
        sink.accept("2");
        sleep(300);
        Assert.assertEquals(1, sink.getUnprocessedElements().size());
        Assert.assertEquals(1, consumer.getNumReceived());
        
        try {
            sink.accept("3");
            Assert.fail("IllegalStateException exepcted");
        } catch (IllegalStateException exepcted) {  }
    }
    

    
    @Test
    public void testsuffientBufferSize() throws Exception {
        TestSubscriber<String> consumer = new TestSubscriber<>(1);
        consumer.suspend();
        
        
        ReactiveSink<String> sink = ReactiveSink.buffer(2)
                                                .subscribe(consumer);
        
        sink.accept("1");
        sleep(300);
        Assert.assertEquals(0, sink.getUnprocessedElements().size());
        Assert.assertEquals(1, consumer.getNumReceived());
                
        sink.accept("2");
        sleep(300);
        Assert.assertEquals(1, sink.getUnprocessedElements().size());
        Assert.assertEquals(1, consumer.getNumReceived());
        
        sink.accept("3");
        sleep(300);
        Assert.assertEquals(2, sink.getUnprocessedElements().size());
        Assert.assertEquals(1, consumer.getNumReceived());

        
        consumer.resume();
        sleep(300);
        Assert.assertEquals(0, sink.getUnprocessedElements().size());
        Assert.assertEquals(3, consumer.getNumReceived());
    }
    
    
    @Test
    public void testClose() throws Exception {
        TestSubscriber<String> consumer = new TestSubscriber<>(1);
        
        
        ReactiveSink<String> sink = ReactiveSink.buffer(20)
                                                .subscribe(consumer);
        
        sink.accept("1");
        sink.accept("2");
        sink.accept("3");
        sink.accept("4");
        sleep(300);
        
        sink.shutdownNow();
        
        try {
            sink.accept("5");
            Assert.fail("IllegalStateException expected");
        } catch (IllegalStateException expected) {
            Assert.assertEquals("stream is already closed", expected.getMessage());
        }
    }
    
    
    @Test
    public void testCloseWithUnprocessedElements() throws Exception {
        TestSubscriber<String> consumer = new TestSubscriber<>(1);
        
        consumer.suspend();
        
        ReactiveSink<String> sink = ReactiveSink.buffer(20)
                                                .subscribe(consumer);
        
        sink.accept("1");
        sink.accept("2");
        sink.accept("3");
        sink.accept("4");
        sleep(300);
        
        sink.shutdownNow();
        
        try {
            sink.accept("5");
            Assert.fail("IllegalStateException expected");
        } catch (IllegalStateException expected) {
            Assert.assertEquals("stream is already closed elements which have not been sent: 2, 3, 4", expected.getMessage());
        }
    }
    
    
    
    
    @Test
    public void testCloseGraceful() throws Exception {
        TestSubscriber<String> consumer = new TestSubscriber<>(1);
        
        consumer.suspend();
        
        ReactiveSink<String> sink = ReactiveSink.buffer(20)
                                                .subscribe(consumer);
        
        sink.accept("1");
        sink.accept("2");
        sink.accept("3");
        sink.accept("4");
        sleep(300);
        
        sink.shutdown();
        
        try {
            sink.accept("5");
            Assert.fail("IllegalStateException expected");
        } catch (IllegalStateException expected) {
            Assert.assertEquals("stream is already closed", expected.getMessage());
        }
        
        
        Assert.assertEquals(3, sink.getUnprocessedElements().size());
        Assert.assertEquals(1, consumer.getNumReceived());
        
        consumer.resume();
        sleep(300);
        Assert.assertEquals(0, sink.getUnprocessedElements().size());
        Assert.assertEquals(4, consumer.getNumReceived());

    }
    
    
    
    
    
    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) {
            
        }
    }
}