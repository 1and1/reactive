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

import org.jboss.resteasy.plugins.providers.jackson.ResteasyJacksonProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import net.oneandone.reactive.kafka.EmbeddedKafka;
import net.oneandone.reactive.kafka.EmbeddedZookeeper;


public class KafkaResourceTest {

    private static WebContainer server;
    private static Client client;

    private static EmbeddedZookeeper zookeeper;
    private static EmbeddedKafka kafka;

    
    @BeforeClass
    public static void setUp() throws Exception {
        server = new WebContainer("", 5544);
        server.start();
        
        client = ClientBuilder.newClient()
                              .register(new ResteasyJacksonProvider());

        int zookeeperPort = 8653;
        zookeeper = new EmbeddedZookeeper(zookeeperPort);
        zookeeper.start();
            
        int kafkaPort = 8553;
        kafka = new EmbeddedKafka(ImmutableMap.of("broker.id", Integer.toString(new Random().nextInt(100000)),
                                                  "port", Integer.toString(kafkaPort),
                                                  "log.dirs", new File("kafkalog").getAbsolutePath(),
                                                  "zookeeper.connect", "localhost:" + zookeeperPort));
        kafka.start();
    }



    @AfterClass
    public static void tearDown() throws Exception {
        server.stop();
        client.close();
        zookeeper.shutdown();
        kafka.shutdown();
    }

    
    @Test
    public void testFetchMetaData() throws Exception {

        // request all schemas
        String metaData = client.target(server.getBaseUrl() + "/topics/mytopic/schemas")
                                .request(MediaType.TEXT_PLAIN)
                                .get(String.class);

        Assert.assertTrue(metaData.contains("== application/vnd.example.event.myevent+json =="));
        Assert.assertTrue(metaData.contains("== application/vnd.example.event.myevent.list+json =="));
    }
   
        
    @Test
    public void testEnqueue() throws Exception {

        // submit event
        Response resp = client.target(server.getBaseUrl() + "/topics/mytopic/events")
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
        Response resp = client.target(server.getBaseUrl() + "/topics/mytopic/events")
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
   
}