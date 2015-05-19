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
package net.oneandone.reactive.sse;




import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.oneandone.reactive.ReactiveSource;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.servlet.ServletSsePublisher;

import org.reactivestreams.Publisher;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;




public class EventSinkServlet extends HttpServlet {
    
    private static final long serialVersionUID = -2315647950747518122L;
    private final List<ServerSentEvent> events = Lists.newArrayList(); 

    private final Instant startTime = Instant.now();
    private final Duration initialPause = Duration.ofMillis(500);
    
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        Publisher<ServerSentEvent> publisher = new ServletSsePublisher(req, resp);
        ReactiveSource<ServerSentEvent> source = ReactiveSource.subscribe(publisher);
     
        Duration sleepTime = Duration.between(startTime, Instant.now()).minus(initialPause);
        if (!sleepTime.isNegative()) {
            try {
               Thread.sleep(sleepTime.toMillis()); 
            } catch (InterruptedException ignore) { }
        }
        
        source.consume(event -> addEvent(event));
    }
 
    
    private void addEvent(ServerSentEvent event) {
        synchronized (events) {
            events.add(event);            
        }
    }
    
    private ImmutableList<ServerSentEvent> getEvents() {
        synchronized (events) {
            return ImmutableList.copyOf(events);
        }
    }
    
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/plain");
        resp.getWriter().print(Joiner.on("\r\n").join(getEvents().stream().map(event -> event.getData().get().split("_")[0]).collect(Collectors.toList())).toString());
    }
}