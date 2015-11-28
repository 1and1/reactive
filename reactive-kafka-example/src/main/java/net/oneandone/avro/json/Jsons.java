package net.oneandone.avro.json;


import java.io.ByteArrayInputStream;

import java.util.Map.Entry;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonValue;


import com.google.common.base.Charsets;



class Jsons {

    
    private Jsons() { }
    
    
    public static JsonObject addAttribute(JsonObject jsonObject, String name, String value) {
        
        final JsonReader reader = Json.createReader(new ByteArrayInputStream(jsonObject.toString().getBytes(Charsets.UTF_8)));
        JsonObject json =  reader.readObject();

        JsonObjectBuilder builder = Json.createObjectBuilder();
        
        for (Entry<String, JsonValue> nameValuePair : json.entrySet()) {
            builder.add(nameValuePair.getKey(), nameValuePair.getValue());
        }
        
        builder.add(name, value);
        
        return builder.build();
    }    
}

