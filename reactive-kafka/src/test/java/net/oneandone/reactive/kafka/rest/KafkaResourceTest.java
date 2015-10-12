package net.oneandone.reactive.kafka.rest;





import java.io.File;
import java.util.Random;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import net.oneandone.reactive.kafka.EmbeddedKafka;
import net.oneandone.reactive.kafka.EmbeddedZookeeper;




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
        
        client = ClientBuilder.newClient();

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
        
        Response response = client.target(server.getBaseUrl() + "/topics/test")
                                  .request()
                                  .post(Entity.entity(new AddressChangedEvent("us-r3344434", "myAddress", "add"), "application/vnd.ui.events.addresschanged-v1+json"));      
        
        System.out.println(response.getStatus());
        System.out.println(response.getHeaderString("location"));
    } 
}