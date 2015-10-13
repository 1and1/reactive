package net.oneandone.reactive.kafka.rest.beans;

import java.time.Instant;

import javax.xml.bind.annotation.XmlRootElement;



@XmlRootElement
public class AddressChangedEvent {

    public String datetime = Instant.now().toString();
    public String accountId;
    public String address;
    public String operation;
    
    
    public AddressChangedEvent() { }

    
    public AddressChangedEvent(String accountId, String address, String operation) {
        this.accountId = accountId;
        this.address = address;
        this.operation = operation;
    }
}