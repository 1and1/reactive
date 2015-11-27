package net.oneandone.reactive.kafka.rest;

import java.io.File;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

import javax.ws.rs.client.Client;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlRootElement;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;


@Ignore
public class KafkaResourceTest {
    
    private static EmbeddedZookeeper zookeeper;
    private static EmbeddedKafka kafka;

    private static Client client; 
    private static String uri;

    
    @BeforeClass
    public static void setUp() throws Exception {
        BusinesEventApplication.main(new String[0]);
        uri = "http://localhost:8080";
        
        
        int zookeeperPort = 8653;
        zookeeper = new EmbeddedZookeeper(zookeeperPort);
        zookeeper.start();
          
       
        int kafkaPort = 8553;
        kafka = new EmbeddedKafka(ImmutableMap.<String, String>builder().put("broker.id", "149")
                                                                        .put("port", Integer.toString(kafkaPort))
                                                                        .put("reserved.broker.max.id", "1000")
                                                                        .put("log.dirs", new File("kafkalog").getAbsolutePath())
                                                                        .put("zookeeper.connect", "localhost:" + zookeeperPort).build());
        kafka.start();
       
        
        client = ClientBuilder.newClient(); 
    }



    @AfterClass
    public static void tearDown() throws Exception {
        zookeeper.shutdown();
        kafka.shutdown();
        client.close();
    }
    
    

    
    @Test
    public void simpleTest() {
        
        Client client = ClientBuilder.newClient(); 
        
        String uri = "http://localhost:8080/topics/mytopic/schemas";
        System.out.println(uri);
        
        client.target(uri).request().get(String.class);
        
        client.close();
    }
    
    
    
    @Test
    public void testFetchMetaData() throws Exception {

        // request all schemas
        String metaData = client.target(uri + "/topics/mytopic/schemas")
                                .request(MediaType.TEXT_PLAIN)
                                .get(String.class);

        Assert.assertTrue(metaData.contains("== application/vnd.example.event.myevent+json =="));
        Assert.assertTrue(metaData.contains("== application/vnd.example.event.myevent.list+json =="));
        Assert.assertTrue(metaData.contains("== application/vnd.example.event.customerdatachanged+json =="));
        Assert.assertTrue(metaData.contains("== application/vnd.example.event.customerdatachanged.list+json =="));
        Assert.assertTrue(metaData.contains("== application/vnd.example.mail.mailsend+json =="));
        Assert.assertTrue(metaData.contains("== application/vnd.example.mail.mailsend.list+json =="));
        
    }
   
        
    @Test
    public void testEnqueue() throws Exception {

        // submit event
        Response resp = client.target(uri + "/topics/mytopic/events")
                              .request()
                              .post(Entity.entity(new CustomerChangedEvent("44545453"), 
                                    "application/vnd.example.event.customerdatachanged+json"));
        
        Assert.assertTrue((resp.getStatus() / 100) == 2);
        String uri = resp.getHeaderString("location");
        Assert.assertNotNull(uri);
        resp.close();

        
        // and check if submitted
        String event = client.target(uri)
                             .request(MediaType.APPLICATION_JSON)
                             .get(String.class);      
        System.out.println(event);
    } 
    
    
    
    
    @Test
    public void testEnqueueBatch() throws Exception {

        // submit event
        Response resp = client.target(uri + "/topics/mytopic/events")
                              .request()
                              .post(Entity.entity(new CustomerChangedEvent[] { new CustomerChangedEvent("44545453"), new CustomerChangedEvent("454502") },
                                    "application/vnd.example.event.customerdatachanged.list+json"));
        
        Assert.assertTrue((resp.getStatus() / 100) == 2);
        String uri = resp.getHeaderString("location");
        Assert.assertNotNull(uri);
        resp.close();

        
        // and check if submitted
        String event = client.target(uri)
                             .request(MediaType.APPLICATION_JSON)
                             .get(String.class);      
        System.out.println(event);
    } 
    

    
    @Test
    public void testEnqueueWithIdl() throws Exception {

        // submit event
        Response resp = client.target(uri + "/topics/mytopic/events")
                              .request()
                              .post(Entity.entity(new MailSentEvent("44545453"), 
                                    "application/vnd.example.mail.mailsent+json"));
        
        Assert.assertTrue((resp.getStatus() / 100) == 2);
        String uri = resp.getHeaderString("location");
        Assert.assertNotNull(uri);
        resp.close();

        
        // and check if submitted
        String event = client.target(uri)
                             .request(MediaType.APPLICATION_JSON)
                             .get(String.class);      
        System.out.println(event);
    } 
    
    

        
    @Test
    public void testConsuming() throws Exception {
        
                
        // submit event
        Response resp = client.target(uri + "/topics/topicAA/events")
                              .request()
                              .post(Entity.entity(new CustomerChangedEvent("44545453x"), 
                                    "application/vnd.example.event.customerdatachanged+json"));
        String eventUri = resp.getHeaderString("location");
        Assert.assertNotNull(eventUri);
        resp.close();

        
        System.out.println("curl -i " + uri + "/topics/topicAA/events");
        

        resp = client.target(uri + "/topics/topicAA/events")
                     .request()
                     .post(Entity.entity(new CustomerChangedEvent("545r455443445"), 
                                         "application/vnd.example.event.customerdatachanged+json"));
        eventUri = resp.getHeaderString("location");
        Assert.assertNotNull(eventUri);
        resp.close();


    } 
    

    
    
    
    
    @XmlRootElement 
    public static class CustomerChangedEvent {
        public Header header = new Header();
        public String accountid;
        
        public CustomerChangedEvent() { }
    
         
        public CustomerChangedEvent(String accountid) {
            this.accountid = accountid;
        }
    }

    
    
  
    public static class Header {
        public String eventId = UUID.randomUUID().toString();  // change to timesuuid 
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
    
    

    @XmlRootElement
    public static class MailSentEvent {
        public String id;
        public String date = Instant.now().toString();
        

        public MailSentEvent() { }

        public MailSentEvent(String id) {
            this.id = id;
        }
    }
}