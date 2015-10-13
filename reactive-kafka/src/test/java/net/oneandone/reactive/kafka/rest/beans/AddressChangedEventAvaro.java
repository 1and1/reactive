package net.oneandone.reactive.kafka.rest.beans;

import java.time.Instant;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;



@JsonInclude(Include.ALWAYS) // forces that null values are written 
@XmlRootElement
public class AddressChangedEventAvaro {

    public String datetime = Instant.now().toString();
    public String accountId;
    public String address;
    public StringUnion operation;
    
    
    public AddressChangedEventAvaro() { }

    
    public AddressChangedEventAvaro(String accountId, String address) {
        this(accountId, address, null);
    }
    
    public AddressChangedEventAvaro(String accountId, String address, StringUnion operation) {
        this.accountId = accountId;
        this.address = address;
        this.operation = operation;
    }
}