package net.oneandone.avro.json;


import java.io.InputStream;


import javax.json.Json;
import javax.json.JsonObject;
import javax.json.stream.JsonParser;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

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
    ImmutableList<GenericRecord> toAvroRecords(JsonParser jsonParser);
    

    /** 
     * @param jsonParser  the json parser of the json object to map
     * @return the avro record
     */
   // GenericRecord toAvroRecord(JsonParser jsonParser);
    
   
    /**
     * @param is the input stream returning the json object
     * @return the avro record list
     */
    default ImmutableList<GenericRecord> toAvroRecords(InputStream is) {
        return toAvroRecords(Json.createParser(is));
    }
    
    
    JsonObject toJson(GenericRecord avroRecord);
    
    
    default byte[] toBinaryJson(GenericRecord avroRecord) {
        return toJson(avroRecord).toString().getBytes(Charsets.UTF_8);
    }
}