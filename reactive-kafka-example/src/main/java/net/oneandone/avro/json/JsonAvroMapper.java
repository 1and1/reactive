package net.oneandone.avro.json;


import java.io.InputStream;


import javax.json.Json;
import javax.json.JsonObject;
import javax.json.stream.JsonParser;

import org.apache.avro.Schema;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;


/**
 * Mapper which maps between json and avro records
 *
 */
public interface JsonAvroMapper {

    boolean isCollectionMapper();
    
    Schema getSchema();
    
    String getMimeType();
    
    /** 
     * @param jsonParser  the json parser of the json object to map
     * @return the avro record list
     */
    ImmutableList<AvroMessage> toAvroMessages(JsonParser jsonParser);
    

    /**
     * @param is the input stream returning the json object
     * @return the avro record list
     */
    default ImmutableList<AvroMessage> toAvroMessage(InputStream is) {
        return toAvroMessages(Json.createParser(is));
    }
    
    
    
    JsonObject toJson(AvroMessage avroMessage);
    
    
    default byte[] toBinaryJson(AvroMessage avroMessage) {
        return toJson(avroMessage).toString().getBytes(Charsets.UTF_8);
    }
}