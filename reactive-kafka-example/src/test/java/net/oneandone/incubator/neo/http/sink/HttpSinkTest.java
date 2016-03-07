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
package net.oneandone.incubator.neo.http.sink;



import java.io.File;


import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.annotation.XmlRootElement;

import org.glassfish.jersey.client.JerseyClientBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.unitedinternet.mam.incubator.hammer.http.sink.MamHttpSink;

import jersey.repackaged.com.google.common.collect.Lists;
import net.oneandone.incubator.neo.http.sink.HttpSink;
import net.oneandone.incubator.neo.http.sink.HttpSink.Method;
import net.oneandone.incubator.neo.http.sink.HttpSink.Submission;
import net.oneandone.incubator.neo.http.sink.PersistentSubmission.PersistentSubmissionTask;
import net.oneandone.incubator.neotest.WebServer;



public class HttpSinkTest {
    
    private TestServlet servlet = new TestServlet();
    private WebServer server;
    
    
    @Before
    public void setUp() throws Exception {
        server = WebServer.withServlet(servlet)
                          .start();
    }
    
    @After
    public void tearDown() throws Exception {
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
            if (req.getParameter("delayMillis") != null) {
                try {
                    Thread.sleep(Long.parseLong(req.getParameter("delayMillis")));
                } catch (InterruptedException ignore) { }
            }
            
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
        
        HttpSink sink = HttpSink.target(server.getBasepath() + "rest/topics")
                                .withClient(client)
                                .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                .open();
        Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
        Assert.assertEquals(Submission.State.COMPLETED, submission.getState());
        
        sink.close();
        client.close();
    }


    @Test
    public void testSuccess() throws Exception {
        HttpSink sink = HttpSink.target(server.getBasepath() + "rest/topics")
                                .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                .open();
        Submission submission = sink.submit(new CustomerChangedEvent(44545453), MediaType.valueOf("application/vnd.example.event.customerdatachanged+json"));
        Assert.assertEquals(Submission.State.COMPLETED, submission.getState());
        
        sink.close();
    }
  
  
    @Test
    public void testSuccesAsync() throws Exception {
        
        HttpSink sink = HttpSink.target(server.getBasepath() + "rest/topics?delayMillis=300")
                                .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                .open();
        CompletableFuture<Submission> submission = sink.submitAsync(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
        Assert.assertFalse(submission.isDone());
        
        try {
            Thread.sleep(1500);
        } catch (InterruptedException ignore) { }
        
        Assert.assertTrue(submission.isDone());
        Assert.assertEquals(Submission.State.COMPLETED, submission.get().getState());
        
        
        sink.close();
    }
  
    
    @Test
    public void testPersistentSuccess() throws Exception {
        HttpSink sink = HttpSink.target(server.getBasepath() + "rest/topics")
                                .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(110)))
                                .withPersistency(Files.createTempDir())
                                .open();
        Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
        Assert.assertEquals(Submission.State.COMPLETED, submission.getState());
        
        Assert.assertFalse(((PersistentSubmission) submission).getSubmissionDir().asFile().exists());
        sink.close();
    }
  
    
    @Test
    public void testServerErrorRetriesExceeded() throws Exception {
         HttpSink sink = HttpSink.target(server.getBasepath() + "rest/topics?status=500")
                                 .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(150)))
                                 .open();
         Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
         Assert.assertEquals(Submission.State.PENDING, submission.getState());
         Assert.assertEquals(submission, sink.getPendingSubmissions().iterator().next());
        
         try {
             Thread.sleep(1000);
         } catch (InterruptedException ignore) { }
        
         Assert.assertEquals(1, sink.getMetrics().getNumDiscarded().getCount());
         Assert.assertEquals(2, sink.getMetrics().getNumRetries().getCount());
         Assert.assertEquals(0, sink.getMetrics().getNumSuccess().getCount());
        
         sink.close();
    }


    @Test
    public void testRejectingError() throws Exception {
         HttpSink sink = HttpSink.target(server.getBasepath() + "rest/topics?status=400")
                                 .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                 .open();
         
         try {
             sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
             Assert.fail("BadRequestException expected");
         } catch (BadRequestException expected) { }
        
         Assert.assertEquals(1, sink.getMetrics().getNumDiscarded().getCount());
         Assert.assertEquals(0, sink.getMetrics().getNumRetries().getCount());
         Assert.assertEquals(0, sink.getMetrics().getNumSuccess().getCount());
        
         sink.close();
    }
    

    @Test
    public void testServerErrorIncompletedRetries() throws Exception {
         HttpSink sink = HttpSink.target(server.getBasepath() + "rest/topics?status=500")
                                 .withRetryAfter(Duration.ofMillis(100), Duration.ofMillis(110), Duration.ofSeconds(2))
                                 .open();
         Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
         Assert.assertEquals(Submission.State.PENDING, submission.getState());

        
         try {
             Thread.sleep(1000);
         } catch (InterruptedException ignore) { }
        
         Assert.assertEquals(1, sink.getPendingSubmissions().size());
         Assert.assertEquals(2, sink.getMetrics().getNumRetries().getCount());
         Assert.assertEquals(0, sink.getMetrics().getNumSuccess().getCount());
         Assert.assertEquals(0, sink.getMetrics().getNumDiscarded().getCount());

         try {
             Thread.sleep(2000);
         } catch (InterruptedException ignore) { }
         
         Assert.assertEquals(0, sink.getPendingSubmissions().size());
         Assert.assertEquals(3, sink.getMetrics().getNumRetries().getCount());
         
         
         sink.close();
    }
  
    
    @Test
    public void testServerErrorRetryWithSuccess() throws Exception {
        servlet.setErrorsToThrow(2);
        
        HttpSink sink = HttpSink.target(server.getBasepath() + "rest/topics")
                                .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(110)))
                                .open();
        Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
        Assert.assertEquals(Submission.State.PENDING, submission.getState());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) { }
        
        Assert.assertEquals(0, sink.getMetrics().getNumDiscarded().getCount());
        Assert.assertEquals(2, sink.getMetrics().getNumRetries().getCount());
        Assert.assertEquals(1, sink.getMetrics().getNumSuccess().getCount());
        
        sink.close();
    }


    @Test
    public void testPersistentServerErrorRetryWithSuccess() throws Exception {
        servlet.setErrorsToThrow(2);
        
        HttpSink sink = HttpSink.target(server.getBasepath() + "rest/topics")
                                .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(110)))
                                .withPersistency(Files.createTempDir())
                                .open();
        Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
        Assert.assertEquals(Submission.State.PENDING, submission.getState());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) { }
        
        Assert.assertEquals(0, sink.getMetrics().getNumDiscarded().getCount());
        Assert.assertEquals(2, sink.getMetrics().getNumRetries().getCount());
        Assert.assertEquals(1, sink.getMetrics().getNumSuccess().getCount());
        
        Assert.assertFalse(((PersistentSubmission) submission).getSubmissionDir().asFile().exists());
        sink.close();
    }


    @Test
    public void testPersistentServerErrorRetryWithClose() throws Exception {
        servlet.setErrorsToThrow(2);
        
        HttpSink sink = HttpSink.target(server.getBasepath() + "rest/topics")
                                .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofHours(2)))
                                .withPersistency(Files.createTempDir())
                                .open();
        Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
        Assert.assertEquals(Submission.State.PENDING, submission.getState());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) { }
        
        Assert.assertEquals(0, sink.getMetrics().getNumDiscarded().getCount());
        Assert.assertEquals(1, sink.getMetrics().getNumRetries().getCount());
        Assert.assertEquals(0, sink.getMetrics().getNumSuccess().getCount());
        
        ImmutableSet<Submission> submissions = sink.getPendingSubmissions();
        Assert.assertEquals(1, submissions.size());
        
        sink.close();
        
        File[] files = ((PersistentSubmission) submissions.iterator().next()).getSubmissionDir().asFile().listFiles();
        List<File> fileList = Lists.newArrayList(files);
        Collections.sort(fileList);

        String content = Joiner.on("\n").join(Files.readLines(fileList.get(2), Charsets.UTF_8));
        Assert.assertTrue(content.trim().contains("numTrials=2"));  
    }

    
    @Test
    public void testServerErrorPersistentRetryWithSuccess() throws Exception {
        servlet.setErrorsToThrow(2);
        
        
        HttpSink sink = HttpSink.target(server.getBasepath() + "rest/topics")
                                .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100), Duration.ofMillis(100)))
                                .withPersistency(Files.createTempDir())
                                .open();
        Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
        Assert.assertEquals(Submission.State.PENDING, submission.getState());
        
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) { }
        
        Assert.assertEquals(Submission.State.COMPLETED, submission.getState());
        Assert.assertEquals(0, sink.getMetrics().getNumDiscarded().getCount());
        Assert.assertEquals(2, sink.getMetrics().getNumRetries().getCount());
        Assert.assertEquals(1, sink.getMetrics().getNumSuccess().getCount());
        
        sink.close();
    }

    
    @Test
    public void testClientError() throws IOException {
        HttpSink sink = HttpSink.target(server.getBasepath() + "rest/topics?status=400")
                                .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                .open();
        
        try {
            sink.accept(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
            Assert.fail("BadRequestException expected");
            
            
        } catch (BadRequestException expected) { 

    	} catch (RuntimeException rt) {
    		System.out.println("RT: " + rt);
    		throw rt;
    	}

            
        sink.close();
    }

    
    @Test
    public void testMaxBufferSizeExceeded() throws IOException {
        int maxsize = 2;
        
        HttpSink sink = HttpSink.target(server.getBasepath() + "rest/topics?status=500")
                                .withRetryAfter(ImmutableList.of(Duration.ofHours(2)))
                                .withRetryBufferSize(maxsize)
                                .open();
        
        for (int i = 0; i < maxsize; i++)
        try {
            sink.accept(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
        } catch (BadRequestException expected) { }
            
        
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) { }
        
        Assert.assertEquals(2, sink.getPendingSubmissions().size());

        
        try {
            sink.accept(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
            Assert.fail("IllegalStateException exepcted");
        } catch (IllegalStateException exepcted) { }
        
        sink.close();
    }


    @Test
    public void testReschedulePersistentQueryWithDiscardResult() throws Exception {
        URI target = URI.create("http://localhost:1/rest/topics");
        
        // create query file to simluate former crash
        File queryFile = newPersistentQuery(Files.createTempDir(), 
                                            target, 
                                            Entity.entity(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json"),
                                            ImmutableList.of(Duration.ofMillis(100)),
                                            Instant.now().minus(Duration.ofMinutes(5)));
        
        Assert.assertEquals(1, queryFile.getParentFile().listFiles().length);
        
        
        
        HttpSink sink = HttpSink.target(target)
                                .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                .withPersistency(getGlobalSubmissionsDir(queryFile))
                                .open();
        
        
        try {
            Thread.sleep(3000);
        } catch (InterruptedException ignore) { }
        
        Assert.assertEquals(1, sink.getMetrics().getNumDiscarded().getCount());
        Assert.assertEquals(1, sink.getMetrics().getNumRetries().getCount());
        Assert.assertEquals(0, sink.getMetrics().getNumSuccess().getCount());
        
        sink.close();
        
        Assert.assertEquals(0, getSinkSubmissionsDir(queryFile).listFiles().length);
    }

    @Test
    public void testReschedulePersistentQueryWithStillRetryResult() throws Exception {
        URI target = URI.create(server.getBasepath() + "rest/topics?status=500");
        
        // create query file to simulate former crash
        File queryFile = newPersistentQuery(Files.createTempDir(), 
                                            target, 
                                            Entity.entity(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json"),
                                            ImmutableList.of(Duration.ofMillis(100), Duration.ofHours(10)),
                                            Instant.now().minus(Duration.ofMinutes(5)));
        
        Assert.assertEquals(1, queryFile.getParentFile().listFiles().length);
        String content = Joiner.on("\n").join(Files.readLines(queryFile, Charsets.UTF_8));
        Assert.assertTrue(content.contains("retries=100&36000000"));
        Assert.assertTrue(content.contains("numTrials=0"));
        
        
        HttpSink sink = HttpSink.target(target)
                                .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                .withPersistency(getGlobalSubmissionsDir(queryFile))
                                .open();
        
        
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) { }
        
        Assert.assertEquals(0, sink.getMetrics().getNumDiscarded().getCount());
        Assert.assertEquals(1, sink.getMetrics().getNumRetries().getCount());
        Assert.assertEquals(0, sink.getMetrics().getNumSuccess().getCount());
        
        ImmutableSet<Submission> submissions = sink.getPendingSubmissions();
        Assert.assertEquals(1, submissions.size());
        sink.close();
        
        File[] files = ((PersistentSubmission) submissions.iterator().next()).getSubmissionDir().asFile().listFiles();
        List<File> fileList = Lists.newArrayList(files);
        Collections.sort(fileList);
        
        Assert.assertEquals(2, fileList.size());
        content = Joiner.on("\n").join(Files.readLines(fileList.get(0), Charsets.UTF_8));
        Assert.assertTrue(content.contains("numTrials=0"));

        content = Joiner.on("\n").join(Files.readLines(fileList.get(1), Charsets.UTF_8));
        Assert.assertTrue(content.contains("numTrials=1"));
    }
    
    
    @Test
    public void testReschedulePersistentConcurrentAccess() throws Exception {
        
        URI target = URI.create(server.getBasepath() + "rest/topics?status=500");
        
        // create query file to simulate former crash
        File queryFile = newPersistentQuery(Files.createTempDir(), 
                                            target, 
                                            Entity.entity(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json"),
                                            ImmutableList.of(Duration.ofMillis(100), Duration.ofHours(10)),
                                            Instant.now().minus(Duration.ofMinutes(5)));
        
        Assert.assertEquals(1, queryFile.getParentFile().listFiles().length);
        String content = Joiner.on("\n").join(Files.readLines(queryFile, Charsets.UTF_8));
        Assert.assertTrue(content.contains("retries=100&36000000"));
        
       
        
        // open the sink (former query should be processed and failed again -> uri is bad) 
        HttpSink sink = HttpSink.target(target)
                                .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                .withPersistency(getGlobalSubmissionsDir(queryFile))
                                .open();
       
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) { }
        
        
        Assert.assertEquals(1, sink.getPendingSubmissions().size());
        Assert.assertEquals(0, sink.getMetrics().getNumDiscarded().getCount());
        Assert.assertEquals(1, sink.getMetrics().getNumRetries().getCount());
        Assert.assertEquals(0, sink.getMetrics().getNumSuccess().getCount());

        
        

        // start a second, concurrent sink 
        HttpSink sink2 = HttpSink.target(target)
                                 .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                 .withPersistency(getGlobalSubmissionsDir(queryFile))
                                 .open();
       
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) { }
        
        // the crashed query will not be visible for the second sink -> locked by the other one 
        Assert.assertEquals(0, sink2.getPendingSubmissions().size());
        Assert.assertEquals(0, sink2.getMetrics().getNumDiscarded().getCount());
        Assert.assertEquals(0, sink2.getMetrics().getNumRetries().getCount());
        Assert.assertEquals(0, sink2.getMetrics().getNumSuccess().getCount());
       
        
        Assert.assertEquals(1, sink.getPendingSubmissions().size());
        Assert.assertEquals(0, sink.getMetrics().getNumDiscarded().getCount());
        Assert.assertEquals(1, sink.getMetrics().getNumRetries().getCount());
        Assert.assertEquals(0, sink.getMetrics().getNumSuccess().getCount());
        
        ImmutableSet<Submission> submissions = sink.getPendingSubmissions();
        Assert.assertEquals(1, submissions.size());
         
        sink.close();
        
        File[] files = ((PersistentSubmission) submissions.iterator().next()).getSubmissionDir().asFile().listFiles();
        List<File> fileList = Lists.newArrayList(files);
        Collections.sort(fileList);
        
        Assert.assertEquals(2, fileList.size());
        
        content = Joiner.on("\n").join(Files.readLines(fileList.get(0), Charsets.UTF_8));
        Assert.assertTrue(content.contains("numTrials=0"));
        
        content = Joiner.on("\n").join(Files.readLines(fileList.get(1), Charsets.UTF_8));
        Assert.assertTrue(content.contains("numTrials=1"));
        
        sink2.close();
    }
    
    
    @Test
    public void testReschedulePersistentQueryWithSuccessResult() throws Exception {
        
        URI target = URI.create(server.getBasepath() + "rest/topics");
        
        // create a query file to 
        File queryFile = newPersistentQuery(Files.createTempDir(), 
                                            target,
                                            Entity.entity(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json"),
                                            ImmutableList.of(Duration.ofMillis(100)),
                                            Instant.now().minus(Duration.ofMinutes(5)));
        Assert.assertEquals(1, queryFile.getParentFile().listFiles().length);
        
        
        HttpSink sink = HttpSink.target(target)
                                .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                .withPersistency(getGlobalSubmissionsDir(queryFile))
                                .open();
        
        
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignore) { }
        
        Assert.assertEquals(0, sink.getMetrics().getNumDiscarded().getCount());
        Assert.assertEquals(1, sink.getMetrics().getNumRetries().getCount());
        Assert.assertEquals(1, sink.getMetrics().getNumSuccess().getCount());
        
        sink.close();
        
        Assert.assertEquals(0, ((HttpSinkBuilderImpl.PersistentHttpSink) sink).getSubmissionStoreDir().listFiles().length);
    }

    
    @Test
    public void testExtendedHttpSink() throws Exception {
        
        HttpSink sink = MamHttpSink.target(server.getBasepath() + "rest/topics")
                                   .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                   .open();
        Submission submission = sink.submit(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json");
        Assert.assertEquals(Submission.State.COMPLETED, submission.getState());
        
        sink.close();
    }
    

    @Test
    public void testReschedulePersistentPendingQueryWithSuccessResult() throws Exception {
        
        URI target = URI.create(server.getBasepath() + "rest/topics");
        
        // create a query file to 
        File queryFile = newPersistentQuery(Files.createTempDir(), 
                                            target,
                                            Entity.entity(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json"),
                                            ImmutableList.of(Duration.ofMinutes(0), Duration.ofMinutes(20), Duration.ofSeconds(91)),
                                            Instant.now().minus(Duration.ofMinutes(5)),
                                            2,
                                            Instant.now().minus(Duration.ofSeconds(90)));
        Assert.assertEquals(1, queryFile.getParentFile().listFiles().length);
        
        
        HttpSink sink = HttpSink.target(target)
                                .withRetryAfter(ImmutableList.of(Duration.ofMillis(100), Duration.ofMillis(100)))
                                .withPersistency(getGlobalSubmissionsDir(queryFile))
                                .open();
        
        
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignore) { }
        
        Assert.assertEquals(0, sink.getMetrics().getNumDiscarded().getCount());
        Assert.assertEquals(1, sink.getMetrics().getNumRetries().getCount());
        Assert.assertEquals(1, sink.getMetrics().getNumSuccess().getCount());
        
        sink.close();
        
        Assert.assertEquals(0, getSinkSubmissionsDir(queryFile).listFiles().length);
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

    private static File newPersistentQuery(File dir, URI uri, Entity<?> entity, ImmutableList<Duration> delays, Instant lastModified) {
        return newPersistentQuery(dir, uri, entity, delays, lastModified, 0, Instant.now()); 
    }

 
    private static File newPersistentQuery(File dir, URI uri, Entity<?> entity, ImmutableList<Duration> delays, Instant lastModified, int trials, Instant dateLastTrial) {
        try {
        	PersistentSubmission.SubmissionsStore submissionsDir = new PersistentSubmission.SubmissionsStore(dir, uri, Method.POST);
            PersistentSubmission persistentSubmission = new PersistentSubmission(UUID.randomUUID().toString(), uri, Method.POST, entity, ImmutableSet.of(404), delays, submissionsDir);    
            PersistentSubmissionTask task = (PersistentSubmissionTask) persistentSubmission.openAsync().get();
            task.onReleased();
            
            File submissionFile = task.getFile();
            submissionFile.setLastModified(lastModified.toEpochMilli());
    
            persistentSubmission.getSubmissionDir().close();
            
            return submissionFile;
        } catch (ExecutionException | InterruptedException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private static File getSinkSubmissionsDir(File submissionFile) {
        return submissionFile.getParentFile()    // submission dir 
                             .getParentFile();   // uri-specific submissions dir
    }
    
    private static File getGlobalSubmissionsDir(File submissionFile) {
        return getSinkSubmissionsDir(submissionFile).getParentFile();   // global submissions dir
    }
}