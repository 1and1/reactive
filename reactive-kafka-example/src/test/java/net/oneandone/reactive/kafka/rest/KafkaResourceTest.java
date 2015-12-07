package net.oneandone.reactive.kafka.rest;

import java.io.File;



import java.io.IOException;
import java.time.Instant;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import javax.json.Json;
import javax.ws.rs.client.Client;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlRootElement;
import org.glassfish.jersey.client.JerseyClientBuilder;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.io.CharSource;

import net.oneandone.reactive.ReactiveSource;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.client.ClientSseSource;


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
                                                                        .put("num.partitions", "3")
                                                                        .put("log.dirs", new File("kafkalog").getAbsolutePath())
                                                                        .put("zookeeper.connect", "localhost:" + zookeeperPort).build());
        kafka.start();
       
        
        // Why JerseyClientBuilder?-> force to use jersey (depending on the classpath resteasy client may used instead)
        client = JerseyClientBuilder.createClient();
    }


    @AfterClass
    public static void tearDown() throws Exception {
        zookeeper.shutdown();
        kafka.shutdown();
        client.close();
    }
    
    

    
    @Test
    public void allScenarios() throws IOException {
        
        
        String topicName = "test" + new Random().nextInt(9999999); 
        
        
        
        ///////////////////////
        // (1) first fetch meta data
        
        
        // request topic info
        String topic = client.target(uri + "/topics/" + topicName)
                             .request()
                             .get(String.class);

        Assert.assertTrue(topic.contains("/topics/" + topicName + "/schemas"));
        
       

        // request all schemas
        String metaData = client.target(uri + "/topics/" + topicName + "/schemas")
                                .request(MediaType.TEXT_PLAIN)
                                .get(String.class);

        Assert.assertTrue(metaData.contains("== application/vnd.example.event.myevent+json =="));
        Assert.assertTrue(metaData.contains("== application/vnd.example.event.myevent.list+json =="));
        Assert.assertTrue(metaData.contains("== application/vnd.example.event.customerdatachanged+json =="));
        Assert.assertTrue(metaData.contains("== application/vnd.example.event.customerdatachanged.list+json =="));
        Assert.assertTrue(metaData.contains("== application/vnd.example.mail.mailsent+json =="));
        Assert.assertTrue(metaData.contains("== application/vnd.example.mail.mailsent.list+json =="));
        
        
        
        
        
        
        
        ///////////////////////
        // (2) submit events
        
        
        
        
        // submit event based on avro schema
        Response resp = client.target(uri + "/topics/" + topicName + "/events")
                              .request()
                              .post(Entity.entity(new CustomerChangedEvent(44545453), "application/vnd.example.event.customerdatachanged+json"));

        Assert.assertTrue((resp.getStatus() / 100) == 2);
        String resourceUri1 = resp.getHeaderString("location");
        Assert.assertNotNull(resourceUri1);
        resp.close();
 
        // and check if submitted
        CustomerChangedEvent ccEvent = client.target(resourceUri1)
                                             .request("application/vnd.example.event.customerdatachanged+json")
                                             .get(CustomerChangedEvent.class);
        Assert.assertEquals(44545453, ccEvent.accountid);
        
        
     
        
        // submit event based on avro idl
        resp = client.target(uri + "/topics/" + topicName + "/events")
                     .request()
                     .post(Entity.entity(new MailSentEvent("4454545z3"), "application/vnd.example.mail.mailsent+json"));
        
        Assert.assertTrue((resp.getStatus() / 100) == 2);
        String resourceUri2 = resp.getHeaderString("location");
        Assert.assertNotNull(resourceUri2);
        resp.close();

        
        // and check if submitted
        MailSentEvent msEvent = client.target(resourceUri2)
                                      .request("*/*")
                                      .get(MailSentEvent.class);      
        Assert.assertEquals("4454545z3", msEvent.id);
        
        
        
        
        
        // submit batch event
        resp = client.target(uri + "/topics/" + topicName + "/events")
                     .request()
                     .post(Entity.entity(new CustomerChangedEvent[] { new CustomerChangedEvent(5555), new CustomerChangedEvent(4544) }, "application/vnd.example.event.customerdatachanged.list+json"));
        
        Assert.assertTrue((resp.getStatus() / 100) == 2);
        String resourceUri3 = resp.getHeaderString("location");
        Assert.assertNotNull(resourceUri3);
        resp.close();

        
        // and check if submitted
        CustomerChangedEvent[] ccEvents = client.target(resourceUri3)
                                                .request("application/vnd.example.event.customerdatachanged.list+json")
                                                .get(CustomerChangedEvent[].class);
        Assert.assertTrue((ccEvents[0].accountid == 5555) || (ccEvents[1].accountid == 5555));
        Assert.assertTrue((ccEvents[0].accountid == 4544) || (ccEvents[1].accountid == 4544));
        
        
        // and check if submitted (with wildcard type)
        ccEvents = client.target(resourceUri3)
                                                .request("*/*")
                                                .get(CustomerChangedEvent[].class);
        Assert.assertTrue((ccEvents[0].accountid == 5555) || (ccEvents[1].accountid == 5555));
        Assert.assertTrue((ccEvents[0].accountid == 4544) || (ccEvents[1].accountid == 4544));
        

        
        
        
        ///////////////////////
        // (3) consume single event

        // query with newer schema (-> Avro Schema Resolution)
        CustomerChangedEventV2 ccEventV2 = client.target(resourceUri1)
                                                 .request("application/vnd.example.event.customerdatachanged-v2+json")
                                                 .get(CustomerChangedEventV2.class);      
        Assert.assertEquals(44545453, ccEventV2.accountid);
        

       
        
        
        ///////////////////////
        // (4) consume event stream
            
      
        ReactiveSource<ServerSentEvent> reactiveSource = new ClientSseSource(uri + "/topics/" + topicName + "/events").open();    
        reactiveSource.read();
        reactiveSource.read();
        reactiveSource.read();
        ServerSentEvent sse = reactiveSource.read();
        
        String lastEventId = sse.getId().get();

        
        // submit 2 more events
        client.target(uri + "/topics/" + topicName + "/events").request().post(Entity.entity(new CustomerChangedEvent(2234334), "application/vnd.example.event.customerdatachanged+json"), String .class);
        client.target(uri + "/topics/" + topicName + "/events").request().post(Entity.entity(new CustomerChangedEvent(223323), "application/vnd.example.event.customerdatachanged+json"), String .class);
        reactiveSource.read();
        reactiveSource.read();
        reactiveSource.close();
        
        
        Set<Integer> receivedIds = Sets.newHashSet(2234334, 223323);
        reactiveSource = new ClientSseSource(uri + "/topics/" + topicName + "/events").withLastEventId(lastEventId).open();    
        ServerSentEvent sse1 = reactiveSource.read();
        receivedIds.remove(Json.createReader(CharSource.wrap(sse1.getData().get()).openStream()).readObject().getInt("accountid"));
        
        ServerSentEvent sse2 = reactiveSource.read();
        receivedIds.remove(Json.createReader(CharSource.wrap(sse2.getData().get()).openStream()).readObject().getInt("accountid"));

        reactiveSource.close();
        
        Assert.assertTrue(receivedIds.isEmpty());
        
        
        
    
        // consume with filter
        reactiveSource = new ClientSseSource(uri + "/topics/" + topicName + "/events?q.data.accountid.eq=2234334").open();    
        ServerSentEvent sseF = reactiveSource.read();
        Assert.assertTrue(sseF.getData().get().contains("\"accountid\":2234334"));
        
        reactiveSource.close();
        
        
       
        
        
        
        System.out.println("done");
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

    

    
    @XmlRootElement 
    public static class CustomerChangedEventV2 {
        public Header header = new Header();
        public long accountid;
        
        public CustomerChangedEventV2() { }
    
         
        public CustomerChangedEventV2(long accountid) {
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