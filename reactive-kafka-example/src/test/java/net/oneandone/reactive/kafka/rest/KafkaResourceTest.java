package net.oneandone.reactive.kafka.rest;


import javax.ws.rs.client.Client;

import javax.ws.rs.client.ClientBuilder;

import org.junit.Test;

public class KafkaResourceTest {
    
    @Test
    public void simpleTest() {
        int port = 4854;
        
        RestServiceMain.main(new String[]{ "server.port=" + port });

        Client client = ClientBuilder.newClient(); 
        
        String uri = "http://localhost:" + port + "/topics/mytopic/schemas";
        System.out.println(uri);
        
        client.target(uri).request().get(String.class);
        
        client.close();
    }
}