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
package net.oneandone.commons.incubator.neo.httpsink;



import java.io.File;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.client.Client;
import javax.xml.bind.annotation.XmlRootElement;

import org.glassfish.jersey.client.JerseyClientBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import net.oneandone.commons.incubator.neo.httpsink.EntityConsumer;
import net.oneandone.commons.incubator.neo.httpsink.EntityConsumer.Submission;
import net.oneandone.commons.incubator.neo.httpsink.HttpSink;
import net.oneandone.commons.incubator.neotest.WebServer;



public class HttpSinkTest {
    
    private static TestServlet servlet = new TestServlet();
    private static WebServer server;

    
    @BeforeClass
    public static void setUp() throws Exception {
        server = WebServer.withServlet(servlet)
                          .start();
    }
    
    @AfterClass
    public static void tearDown() throws Exception {
        server.close();
    }
    
    
    public static final class TestServlet extends HttpServlet {
        private static final long serialVersionUID = -1136798776740561582L;

        private final AtomicInteger errorsToThrow = new AtomicInteger(0);
        
        public void setErrorsToThrow(int num) {
            errorsToThrow.set(num);
        }
        
        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            
            if (errorsToThrow.get() > 0) {
                errorsToThrow.decrementAndGet();
                resp.sendError(500);
                return;
            }
            
            
            if (req.getParameter("status") != null) {
                int status = Integer.parseInt(req.getParameter("status"));
                resp.setStatus(status);
                return;
            } 
            
            resp.setStatus(200);
        }
    }

    
    
    
    @Test
    public void testWithClient() throws Exception {
        
        Client client = JerseyClientBuilder.createClient();
        
        EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics")
                                      .withClient(client)
                                      .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                      .open();
        Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
        Assert.assertEquals(Submission.Status.COMPLETED, submission.getStatus());
        
        sink.close();
        client.close();
    }

    
    @Test
    public void testSuccess() throws Exception {
        
        EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics")
                                      .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                      .open();
        Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
        Assert.assertEquals(Submission.Status.COMPLETED, submission.getStatus());
        
        sink.close();
    }
  
  


    
    @Test
    public void testServerErrorRetriesExceeded() throws Exception {
         EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics?status=500")
                                       .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                       .open();
         Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
         Assert.assertEquals(Submission.Status.PENDING, submission.getStatus());
         
        
         try {
             Thread.sleep(1000);
         } catch (InterruptedException ignore) { }
        
         Assert.assertEquals(1, sink.getNumDiscarded());
         Assert.assertEquals(2, sink.getNumRetries());
         Assert.assertEquals(0, sink.getNumSuccess());
        
         sink.close();
    }

    
    @Test
    public void testRejectingError() throws Exception {
         EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics?status=400")
                                       .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                       .open();
         
         try {
             sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
             Assert.fail("BadRequestException expected");
         } catch (BadRequestException expected) { }
        
         Assert.assertEquals(1, sink.getNumRejected());
         Assert.assertEquals(0, sink.getNumDiscarded());
         Assert.assertEquals(0, sink.getNumRetries());
         Assert.assertEquals(0, sink.getNumSuccess());
        
         sink.close();
    }
    
    @Test
    public void testServerErrorIncompletedRetries() throws Exception {
         EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics?status=500")
                                       .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100), Duration.ofMillis(5000)))
                                       .open();
         Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
         Assert.assertEquals(Submission.Status.PENDING, submission.getStatus());

        
         try {
             Thread.sleep(1000);
         } catch (InterruptedException ignore) { }
        
         Assert.assertEquals(0, sink.getNumDiscarded());
         Assert.assertEquals(2, sink.getNumRetries());
         Assert.assertEquals(0, sink.getNumSuccess());
        
         sink.close();
    }
  
  
    @Test
    public void testServerErrorRetryWithSuccess() throws Exception {
        servlet.setErrorsToThrow(1);
        
        EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics")
                                      .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                      .open();
        Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
        Assert.assertEquals(Submission.Status.PENDING, submission.getStatus());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) { }
        
        Assert.assertEquals(0, sink.getNumDiscarded());
        Assert.assertEquals(1, sink.getNumRetries());
        Assert.assertEquals(1, sink.getNumSuccess());
        
        sink.close();
    }


    
    
    @Test
    public void testServerErrorPersistentRetryWithSuccess() throws Exception {
        servlet.setErrorsToThrow(2);
        
        EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics")
                                      .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100), Duration.ofMillis(100)))
                                      .withRetryPersistency(new File(new File("."), "retrydir"))
                                      .open();
        Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
        Assert.assertEquals(Submission.Status.PENDING, submission.getStatus());
        
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) { }
        
        Assert.assertEquals(Submission.Status.COMPLETED, submission.getStatus());
        Assert.assertEquals(0, sink.getNumDiscarded());
        Assert.assertEquals(2, sink.getNumRetries());
        Assert.assertEquals(1, sink.getNumSuccess());
        
        sink.close();
    }

    
    
    
    
    
    
    @Test
    public void testClientError() throws IOException {
        
        Client client = JerseyClientBuilder.createClient();
        
        EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics?status=400")
                                      .withClient(client)
                                      .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                      .open();
        
        try {
            sink.accept(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
            Assert.fail("BadRequestException expected");
        } catch (BadRequestException expected) { }
            
        sink.close();
        client.close();
    }

    
    @Test
    public void testReschedulePersistentQuery() throws Exception {
        
        File dir = Files.createTempDir();
        File querysDir = new File("src" + File.separator + "test" + File.separator + "resources" + File.separator + "queries");
        
        for (File queryFile : querysDir.listFiles()) {
            Files.copy(queryFile, new File(dir, queryFile.getName()));
        }
        Assert.assertTrue(dir.listFiles().length > 0);
        
        EntityConsumer sink = HttpSink.create(server.getBasepath() + "rest/topics")
                                      .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                      .withRetryPersistency(dir)
                                      .open();
        
        
        try {
            Thread.sleep(3000);
        } catch (InterruptedException ignore) { }
        
        Assert.assertEquals(0, sink.getNumRejected());
        Assert.assertEquals(1, sink.getNumDiscarded());
        Assert.assertEquals(1, sink.getNumRetries());
        Assert.assertEquals(0, sink.getNumSuccess());
        
        sink.close();
        
        Assert.assertTrue(dir.listFiles().length == 0);
        dir.delete();
    }
    
    
    @XmlRootElement 
    public static class CustomerChangedEvent {
        public Header header = new Header();
        public int accountid;
        
        public CustomerChangedEvent() { }
    
         
        public CustomerChangedEvent(int accountid) {
            this.accountid = accountid;
        }
    }

        
    
    
  
    public static class Header {
        public String eventId = UUID.randomUUID().toString().replace("-", ""); 
        public String timestamp = Instant.now().toString();
        public AuthenticationInfo authInfo;
        
        
        public Header() {  }
        

        public Header(String eventId, String timestamp, AuthenticationInfo authInfo) { 
            this.eventId = eventId;
            this.timestamp = timestamp;
            this.authInfo = authInfo;
        }
        
        
        public Header withAuthInfo(AuthenticationInfo.AuthenticationScheme scheme, String principalname) {
            return new Header(eventId, timestamp, new AuthenticationInfo(scheme, principalname));
        }
    }

    
    public static final class AuthenticationInfo  {
        public static enum AuthenticationScheme { BASIC, CLIENTCERT, DIGEST, PLAIN, FORM, OAUTHBEARER };
        
        public AuthenticationInfo() {  }
        
        public AuthenticationInfo(AuthenticationScheme scheme, String principalname) {
            this.principalname = principalname;
            this.scheme = scheme;
        }
        
        public String principalname;
        public AuthenticationScheme scheme;
    }
}