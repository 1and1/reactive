package net.oneandone.avro.json;


import java.io.InputStream;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.stream.JsonParser;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.ImmutableList;


/**
 * Mapper which maps between json and avro records
 *
 */
public interface JsonAvroMapper {

    Schema getSchema();
    
    SchemaName getSchemaName();
    
    String getMimeType();
    
    /** 
     * @param jsonParser  the json parser of the json object to map
     * @return the avro record
     */
    ImmutableList<GenericRecord> toAvroRecord(JsonParser jsonParser);

    
    /**
     * @param jsonParser  the json parser of the json object to map
     * @return the binary avro record
     */
    ImmutableList<byte[]> toAvroBinaryRecord(JsonParser jsonParser);
    
     
    /**
     * @param is the input stream returning the json object
     * @return the binary avro record
     */
    default ImmutableList<byte[]> toAvroBinaryRecord(InputStream is) {
        return toAvroBinaryRecord(Json.createParser(is));
    }
    
   
    JsonObject toJson(GenericRecord avroRecord);
     
}