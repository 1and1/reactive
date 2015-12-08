package net.oneandone.avro.json;


public class AvroSerializationException extends RuntimeException {

    private static final long serialVersionUID = -8975292396567133489L;
    
    private final String type;


    public AvroSerializationException(String reason, String type, Throwable cause) { 
        super(reason, cause);
        this.type = type;
    } 
    
    public String getType() {
        return type;
    }
}