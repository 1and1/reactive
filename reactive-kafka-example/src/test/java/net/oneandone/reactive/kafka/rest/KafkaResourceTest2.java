package net.oneandone.reactive.kafka.rest;


import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = RestServiceMain.class)
@WebAppConfiguration
@IntegrationTest("server.port=0")
public class KafkaResourceTest2 {

    @Value("${local.server.port}")
    int port;

    
    @Test
    public void simpleTest() {

        Client client = ClientBuilder.newClient(); 
        
        String uri = "http://localhost:" + port + "/topics/mytopic/schemas";
        System.out.println(uri);
        
        client.target(uri).request().get(String.class);
        
        client.close();
    }
}