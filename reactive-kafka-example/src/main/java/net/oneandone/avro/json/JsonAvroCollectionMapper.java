package net.oneandone.avro.json;


import java.util.List;
import java.util.stream.Collectors;

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
    private final SchemaName schemaName;

    
    public JsonAvroCollectionMapper(JsonAvroEntityMapper entityMapper) {
        this.entityMapper = entityMapper;
        this.schemaName = new SchemaName(entityMapper.getSchemaName().getNamespace(), entityMapper.getSchemaName().getName() + "_list");
        this.mimeType  = "application/vnd." + Joiner.on(".").join(schemaName.getNamespace(), schemaName.getName()) + ".list+json";
    }
    
    @Override
    public Schema getSchema() {
        return entityMapper.getSchema();
    }
    
    @Override
    public SchemaName getSchemaName() {
        return schemaName;
    }

    @Override
    public String getMimeType() {
        return mimeType;
    }
    
    @Override
    public ImmutableList<byte[]> toAvroBinaryRecord(JsonParser jsonParser) {
        return ImmutableList.copyOf(toAvroRecord(jsonParser).stream()
                                                            .map(record -> Avros.serializeAvroMessage(record, entityMapper.getSchema()))
                                                            .collect(Collectors.toList()));
    }
    
    
    @Override
    public ImmutableList<GenericRecord> toAvroRecord(JsonParser jsonParser) {

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
                avroRecords.add(entityMapper.toSingleAvroRecord(jsonParser));
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