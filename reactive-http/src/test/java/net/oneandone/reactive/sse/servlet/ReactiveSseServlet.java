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
package net.oneandone.reactive.sse.servlet;




import java.io.IOException;
import java.time.Duration;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.oneandone.reactive.sse.ServerSentEvent;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import com.google.common.collect.Maps;




public class ReactiveSseServlet extends HttpServlet {
    private static final long serialVersionUID = -7372081048856966492L;

    private final Broker broker = new Broker();
        
  
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.startAsync();
        
        resp.setStatus(200);
        resp.flushBuffer();
        
        Publisher<ServerSentEvent> publisher = new ServletSsePublisher(req);
        broker.registerPublisher(req.getPathInfo(), publisher);
    }
    
    
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.startAsync();
        
        resp.setContentType("text/event-stream");
        Subscriber<ServerSentEvent> subscriber = new ServletSseSubscriber(resp, Duration.ofSeconds(5));
        broker.registerSubscriber(req.getPathInfo(), subscriber, 60 * 1000);
    }
    
    
    
    private static final class Broker {
        private final Map<String, Publisher<ServerSentEvent>> publishers = Maps.newConcurrentMap();

        public synchronized void registerPublisher(String id, Publisher<ServerSentEvent> publisher) {
            publishers.put(id, publisher);
            notifyAll();
        }
        
        public synchronized void registerSubscriber(String id, Subscriber<ServerSentEvent> subscriber, int maxMillis) {
            long start = System.currentTimeMillis();
            
            while (System.currentTimeMillis() < (start + maxMillis)) {
                Publisher<ServerSentEvent> publisher = publishers.get(id);
                
                if (publisher == null) {
                    try {
                        wait(100);
                    } catch (InterruptedException ignore) { } 
                    
                } else {
                    publisher.subscribe(subscriber);
                    return;
                }
            }
            
            throw new RuntimeException("Timeout " + maxMillis + " millis exceeded");
        }
    }
}