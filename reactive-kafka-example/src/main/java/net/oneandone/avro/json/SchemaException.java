package net.oneandone.avro.json;

import java.util.function.Supplier;

public class SchemaException extends RuntimeException {

    private static final long serialVersionUID = -3894104697627836613L;

    private final String type;

    @Deprecated
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