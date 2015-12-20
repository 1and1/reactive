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
package net.oneandone.commons.incubator.neotest;

import java.io.Closeable;

import javax.servlet.Servlet;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;


public class WebServer implements Closeable {
    private final String path;
    private final ServerConnector connector;
    private final Server server; 
    
    
    public static WebServerBuilder withServlet(Class<? extends Servlet> servlet) {
        return new WebServerBuilder(new ServletBasedDeployment(servlet), 0, "/");
    }
 
    
    public static WebServerBuilder withServlet(Servlet servlet) {
        return new WebServerBuilder(new ServletBasedDeployment(servlet), 0, "/");
    }
    
    private WebServer(Deployment deployment, int port, String path) {
        this.path = path; 
        
        QueuedThreadPool serverExecutor = new QueuedThreadPool();
        serverExecutor.setName("server");

        server = new Server(serverExecutor);
        connector = new ServerConnector(server);
        connector.setPort(port);
        server.addConnector(connector);
        
        deployment.deployInto(server, path);
    }
    
    
    
    void start() {
        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    
    @Override
    public void close() {
        try {
            server.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public int getLocalport() {
        return connector.getLocalPort(); 
    }
    
    
    public String getBasepath() {
        return "http://localhost:" + getLocalport() + path;
    }
    
    

    public static class WebServerBuilder {
        private final Deployment deployment;
        private final String path;
        private final int port;
        
        private WebServerBuilder(Deployment deployment, int port, String path) {
            this.deployment = deployment;
            this.port = port;
            this.path = path;
        }
        
        public WebServerBuilder path(String path) {
            return new WebServerBuilder(this.deployment, this.port, path);
        }

        public WebServerBuilder port(int port) {
            return new WebServerBuilder(this.deployment, port, this.path);
        }

        
        public WebServer start() {
            WebServer webServer = new WebServer(this.deployment, port, path);
            webServer.start();
            return webServer;
        }
    }
    
    
    
    private interface Deployment  {
        
        void deployInto(Server server, String path);
    }
    
   
    private final static class ServletBasedDeployment implements Deployment {
        private final ServletHolder servletHolder;

        public ServletBasedDeployment(Class<? extends Servlet> servlet) {
            this.servletHolder = new ServletHolder(servlet);
        }
        

        public ServletBasedDeployment(Servlet servlet) {
            this.servletHolder = new ServletHolder(servlet);
        }
        
        @Override
        public void deployInto(Server server, String path) {
            ServletContextHandler context = new ServletContextHandler(server, path);
            context.addServlet(servletHolder, path);
        }
    }
}