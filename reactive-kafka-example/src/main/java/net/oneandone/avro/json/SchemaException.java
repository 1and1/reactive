package net.oneandone.avro.json;


public class SchemaException extends RuntimeException {

    private static final long serialVersionUID = -3894104697627836613L;

    private final String type;


    public SchemaException(String reason) {
        super(reason);
        this.type = null;
    } 
        
    public SchemaException(String reason, String type) {
        super(reason);
        this.type = type;
    } 
    
    public String getType() {
        return type;
    }
}