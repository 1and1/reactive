package net.oneandone.avro.json;


import javax.json.JsonObject;

import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.ImmutableList;




public interface JsonAvroMapper {

    ImmutableList<GenericRecord> toAvroRecord(JsonObject jsonObject);
    
    ImmutableList<byte[]> toAvroBinaryRecord(JsonObject jsonObject);
}