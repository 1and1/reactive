package net.oneandone.avro.json;


import java.util.Iterator;
import java.util.Objects;

import jersey.repackaged.com.google.common.base.Splitter;



public class SchemaName {
    
    private final String namespace;
    private final String name;
     
     
    SchemaName(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }
    
    String getNamespace() {
        return namespace;
    }
       
    String getName() {
        return name;
    }
    

    public static SchemaName valueof(String txt) {
        Iterator<String> it = Splitter.on('\n').split(txt).iterator();
        return new SchemaName(it.next(), it.next()); 
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(namespace, name);
    }
    
    @Override
    public boolean equals(Object other) {
        
        return (other != null) && 
               (other instanceof SchemaName) &&
               (Objects.equals(((SchemaName) other).namespace, namespace)) &&
               (Objects.equals(((SchemaName) other).name, name)); 
    }
    
    @Override
    public String toString() {
        return namespace + "\n" + name;
    }
}