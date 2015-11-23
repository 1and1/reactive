package net.oneandone.avro.json;


import java.io.InputStream;

import javax.json.Json;
import javax.json.stream.JsonParser;

import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.ImmutableList;




public interface JsonAvroMapper {

    ImmutableList<GenericRecord> toAvroRecord(JsonParser jsonParser);
    
    ImmutableList<byte[]> toAvroBinaryRecord(JsonParser jsonParser);
    
    default ImmutableList<byte[]> toAvroBinaryRecord(InputStream is) {
        return toAvroBinaryRecord(Json.createParser(is));
    }
    
}