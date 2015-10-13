package net.oneandone.reactive.kafka.rest;





import java.io.File;


import java.util.Random;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.jboss.resteasy.plugins.providers.jackson.ResteasyJacksonProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import net.oneandone.reactive.kafka.EmbeddedKafka;
import net.oneandone.reactive.kafka.EmbeddedZookeeper;
import net.oneandone.reactive.kafka.rest.beans.AddressChangedEvent;
import net.oneandone.reactive.kafka.rest.beans.AddressChangedEventAvaro;



//@Ignore
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
    public void testSimple() throws Exception {

        // submit event
        Response response = client.target(server.getBaseUrl() + "/topics/test")
                                  .request()
                                  .post(Entity.entity(new AddressChangedEvent("us-r3344434", "myAddress", "add"), "application/vnd.ui.events.user.addressmodified-v1+json"));
        
        Assert.assertTrue((response.getStatus() / 100) == 2);
        String kafkaMessageURI = response.getHeaderString("location");
        response.close();
        
        
        // and check if submitted
        String event = client.target(kafkaMessageURI)
                             .request(MediaType.APPLICATION_JSON)
                             .get(String.class);      
        System.out.println(event);
    } 
    
    
    @Test
    public void testSimpleAvro() throws Exception {

        // submit event
        Response response = client.target(server.getBaseUrl() + "/avro/topics/test")
                                  .request()
                                  .post(Entity.entity(new AddressChangedEventAvaro("us-r3344434", "myAddress"), "application/vnd.ui.events.user.addressmodified-v1+json"));
        String kafkaMessageURI = response.getHeaderString("location");
        response.close();
        
        
        // and check if submitted
        String event = client.target(kafkaMessageURI)
                             .request(MediaType.APPLICATION_JSON)
                             .get(String.class);      
        System.out.println(event);
    } 
}