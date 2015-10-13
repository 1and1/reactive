package net.oneandone.reactive.kafka.rest.beans;


import javax.xml.bind.annotation.XmlRootElement;




@XmlRootElement
public class StringUnion {

    public String string; 

    public static StringUnion valueOf(String value) {
        if (value == null) {
            return null;
        } else {
            StringUnion union = new StringUnion();
            union.string = value;
            return union;
        }
    }
}