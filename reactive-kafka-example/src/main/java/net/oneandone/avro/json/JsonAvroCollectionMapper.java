package net.oneandone.avro.json;


import java.util.List;

import javax.json.JsonObject;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;




public class JsonAvroCollectionMapper implements JsonAvroMapper {
        
    private final JsonAvroEntityMapper entityMapper;
    private final String mimeType;

    
    public JsonAvroCollectionMapper(JsonAvroEntityMapper entityMapper) {
        this.entityMapper = entityMapper;
        this.mimeType  = "application/vnd." + Joiner.on(".").join(entityMapper.getSchema().getNamespace(), entityMapper.getSchema().getName()) + ".list+json";
    }
    
    @Override
    public boolean isCollectionMapper() {
        return true;
    }
    
    @Override
    public Schema getSchema() {
        return entityMapper.getSchema();
    }
    
    @Override
    public String getMimeType() {
        return mimeType;
    }
    
    
    @Override
    public ImmutableList<GenericRecord> toAvroRecords(JsonParser jsonParser) {

        // check initial state
        if (jsonParser.next() != Event.START_ARRAY) {
            throw new IllegalStateException("START_ARRAY event excepted");
        }

        
        final List<GenericRecord> avroRecords = Lists.newArrayList();
        
        
        while (jsonParser.hasNext()) {
            
            switch (jsonParser.next()) {

            case END_ARRAY:
                return ImmutableList.copyOf(avroRecords);

            case START_OBJECT:
                avroRecords.addAll(entityMapper.toAvroRecords(jsonParser));
                break;
                
            default:
            }
        }

        throw new IllegalStateException("END_ARRAY event is missing");
    }

    
    @Override
    public JsonObject toJson(GenericRecord avroRecord) {
        throw new UnsupportedOperationException("toJson(...)");
    }
    
    @Override
    public String toString() {
        return "[" + entityMapper.toString() + "\r\n]";
    }
}