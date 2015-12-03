package net.oneandone.avro.json;

import javax.json.JsonObject;
import javax.json.stream.JsonParser;

import org.apache.avro.Schema;

import com.google.common.collect.ImmutableList;


/**
 * Mapper which maps between json and avro records
 *
 */
interface JsonAvroMapper {

    boolean isCollectionMapper();
    
    Schema getSchema();
    
    String getMimeType();
    
    ImmutableList<AvroMessage> toAvroMessages(JsonParser jsonParser);
    
    JsonObject toJson(AvroMessage avroMessage);
}