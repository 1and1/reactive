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




import java.io.IOException;

import java.util.List;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;




public class EventSinkServlet extends HttpServlet {
    private static final long serialVersionUID = -2315647950747518122L;
    private final List<String> events = Lists.newArrayList(); 

      
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        
        String pauseMillis = req.getParameter("pauseMillis");
        if (pauseMillis != null) {
            try {
                Thread.sleep(Integer.parseInt(pauseMillis));
            } catch (InterruptedException ignore) { }
        }
        
        addEvent(new String(ByteStreams.toByteArray(req.getInputStream()), Charsets.UTF_8));
    }
 
    
    private void addEvent(String data) {
        synchronized (events) {
            events.add(data);            
        }
    }
    
    private ImmutableList<String> getEvents() {
        synchronized (events) {
            return ImmutableList.copyOf(events);
        }
    }
    
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/plain");
        resp.getWriter().print(Joiner.on("\r\n").join(getEvents().stream().map(data -> data.split("_")[0]).collect(Collectors.toList())).toString());
    }
}