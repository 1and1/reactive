package net.oneandone.reactive.kafka.rest;





import java.io.File;
import java.time.Instant;
import java.util.Map;
import java.util.Random;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlRootElement;

import org.jboss.resteasy.plugins.providers.jackson.ResteasyJacksonProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import net.oneandone.reactive.kafka.EmbeddedKafka;
import net.oneandone.reactive.kafka.EmbeddedZookeeper;
import net.oneandone.reactive.kafka.rest.KafkaResourceTest.EmailAddressChangedEvent.Operation;
import net.oneandone.reactive.kafka.rest.KafkaResourceTest.EmailSentEvent.AuthenticationScheme;


@Ignore
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

    
    @SuppressWarnings("unchecked")
    @Test
    public void testFetchMetaData() throws Exception {

        // fetch root representation
        Map<String, Object> root = client.target(server.getBaseUrl())
                                         .request(MediaType.APPLICATION_JSON)
                                         .get(new GenericType<Map<String, Object>>() { });

        // to get schema uri
        String schemaUri = (((Map<String, String>) ((Map<String, Object>) root.get("_links")).get("schemas")).get("href"));
        
        
        // request all schemas
        String metaData = client.target(schemaUri)
                                .request(MediaType.TEXT_PLAIN)
                                .get(String.class);

        Assert.assertTrue(metaData.contains("== application/vnd.ui.mam.event.mailbox.mailmoved+json =="));
        Assert.assertTrue(metaData.contains("== application/vnd.ui.mam.event.mailbox.mailmoved.list+json =="));
    }
        
    
    @Test
    public void testEnqueue() throws Exception {

        // submit event
        Response response = client.target(server.getBaseUrl() + "/topics/mytopic/events")
                                  .request()
                                  .post(Entity.entity(new EmailAddressChangedEvent("us-r3344434", "myAddress", Operation.ADDED), 
                                                      "application/vnd.ui.mam.event.mailaccount.emailaddresschanged+json"));
        String kafkaMessageURI = response.getHeaderString("location");
        response.close();
        
        // and check if submitted
        String event = client.target(kafkaMessageURI)
                             .request(MediaType.APPLICATION_JSON)
                             .get(String.class);      
        System.out.println(event);
        
   
        
        
        // submit event
        response = client.target(server.getBaseUrl() + "/topics/mytopic/events")
                         .request()
                         .post(Entity.entity(new EmailSentEvent("44554503", 3, "243.34.34.22", "CN=Hans Herber", AuthenticationScheme.BEARER), 
                                             "application/vnd.ui.mam.event.mailaccount.emailsent+json"));
        kafkaMessageURI = response.getHeaderString("location");
        response.close();
        
        
        
        // and check if submitted
        event = client.target(kafkaMessageURI)
                      .request(MediaType.APPLICATION_JSON)
                      .get(String.class);      
        System.out.println(event);
        
        
        
        
        // submit event
        response = client.target(server.getBaseUrl() + "/topics/mytopic/events")
                         .request()
                         .post(Entity.entity(ImmutableList.of(new EmailMovedEvent("4545403", "345345354", "23322", "34443"), new EmailMovedEvent("4654503", "34545", "23322", "34443")), 
                                             "application/vnd.ui.mam.event.mailbox.emailmoved.list+json"));
        kafkaMessageURI = response.getHeaderString("location");
        response.close();
        
        
        // and check if submitted
        event = client.target(kafkaMessageURI)
                      .request(MediaType.APPLICATION_JSON)
                      .get(String.class);      
        System.out.println(event);
    } 
    
    
 
    
    
    @XmlRootElement
    public static class EmailAddressChangedEvent {
        public static enum Operation { ADDED, REMOVED, UPDATED };
    
        public String timestamp = Instant.now().toString();
        public String mailaccountid;
        public String emailaddress;
        public Operation operation;
        
        
        public EmailAddressChangedEvent() { }
        
        public EmailAddressChangedEvent(String mailaccountid, String emailaddress, Operation operation) {
            this.mailaccountid = mailaccountid;
            this.emailaddress = emailaddress;
            this.operation = operation;
        }
    }
    
    
    
    
    @XmlRootElement
    public static class EmailSentEvent {
        public static enum AuthenticationScheme { BASIC, CLIENTCERT, DIGEST, FORM, BEARER };
    
        public String timestamp = Instant.now().toString();
        public String mailaccountid;
        public int number_of_recipients;
        public String external_ip;
        public String principalname;
        public AuthenticationScheme authenticationscheme;
        
        
        public EmailSentEvent() { }
        
        public EmailSentEvent(String mailaccountid, 
                              int number_of_recipients,
                              String external_ip,
                              String principalname,
                              AuthenticationScheme authenticationscheme) {
            this.mailaccountid = mailaccountid;
            this.number_of_recipients = number_of_recipients;
            this.external_ip = external_ip;
            this.principalname = principalname;
            this.authenticationscheme = authenticationscheme;
        }
    }

    
  
    
    @XmlRootElement
    public static class EmailMovedEvent {
    
        public String timestamp = Instant.now().toString();
        public String mailaccountid;
        public String mailid;
        public String sourcefolderid;
        public String targetfolderid;
        
        
        public EmailMovedEvent() { }
    
        
        public EmailMovedEvent(String mailaccountid,
                              String mailid,
                              String sourcefolderid,
                              String targetfolderid) {
            this.mailaccountid = mailaccountid;
            this.mailid = mailid;
            this.sourcefolderid = sourcefolderid;
            this.targetfolderid = targetfolderid;
        }
    }

    
    

    @XmlRootElement
    public static class StringUnion {
    
        public String string; 
    
        public static StringUnion valueOf(String value) {
            if (value == null) {
                return null;
            } else {
                StringUnion union = new StringUnion();
                union.string = value;
                return union;
            }
        }
    }
}