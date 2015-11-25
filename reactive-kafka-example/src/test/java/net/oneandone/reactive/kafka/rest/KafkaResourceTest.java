package net.oneandone.reactive.kafka.rest;

import javax.ws.rs.client.Client;

import javax.ws.rs.client.ClientBuilder;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


import net.oneandone.reactive.kafka.EmbeddedKafka;
import net.oneandone.reactive.kafka.EmbeddedZookeeper;

public class KafkaResourceTest {
    
    private static EmbeddedZookeeper zookeeper;
    private static EmbeddedKafka kafka;

    
    @BeforeClass
    public static void setUp() throws Exception {
      
        /*int zookeeperPort = 8653;
        zookeeper = new EmbeddedZookeeper(zookeeperPort);
        zookeeper.start();
          */  
        /*
        int kafkaPort = 8553;
        kafka = new EmbeddedKafka(ImmutableMap.of("broker.id", Integer.toString(new Random().nextInt(100000)),
                                                  "port", Integer.toString(kafkaPort),
                                                  "log.dirs", new File("kafkalog").getAbsolutePath(),
                                                  "zookeeper.connect", "localhost:" + zookeeperPort));
        kafka.start();
        */  
    }



    @AfterClass
    public static void tearDown() throws Exception {
        /*
        zookeeper.shutdown();
        kafka.shutdown();*/
    }
    
    

    
    @Test
    public void simpleTest() {
        
        BusinesEventApplication.main(new String[0]);

        Client client = ClientBuilder.newClient(); 
        
        String uri = "http://localhost:8080/topics/mytopic/schemas";
        System.out.println(uri);
        
        client.target(uri).request().get(String.class);
        
        client.close();
    }
}