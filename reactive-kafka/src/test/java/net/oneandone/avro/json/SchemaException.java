package net.oneandone.avro.json;






public class SchemaException extends RuntimeException {

    private static final long serialVersionUID = -3894104697627836613L;

    public SchemaException(String reason) {
        super(reason);
    }
}