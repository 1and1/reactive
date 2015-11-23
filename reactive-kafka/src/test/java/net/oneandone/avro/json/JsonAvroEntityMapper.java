package net.oneandone.avro.json;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;




public class JsonAvroEntityMapper implements JsonAvroMapper {
        
    private final String namespace;
    private final String name;
    private final Schema schema;
    private final JsonObject jsonSchema;
    private final JsonObjectToAvroRecordWriter jsonObjectToAvroWriter;
    
    
    private JsonAvroEntityMapper(String namespace, String name, JsonObject jsonSchema, JsonObjectToAvroRecordWriter jsonObjectToAvroMapper) {
        this.namespace = namespace;
        this.name = name;
        this.jsonSchema = jsonSchema;
        this.schema = new Schema.Parser().parse(jsonSchema.toString());
        this.jsonObjectToAvroWriter = jsonObjectToAvroMapper;
    }
    
    
    public Schema getSchema() {
        return schema;
    }
    
    public String getNamespace() {
        return namespace;
    }
    
    public String getName() {
        return name;
    }

    public String getAbsoluteName() {
        return namespace + '.' + name;
    }
    
    @Override
    public ImmutableList<GenericRecord> toAvroRecord(JsonObject jsonObject) {
        return ImmutableList.of(toSingleAvroRecord(jsonObject));
    }
    
    
    @Override
    public ImmutableList<byte[]> toAvroBinaryRecord(JsonObject jsonObject) {
        return ImmutableList.of(serialize(toSingleAvroRecord(jsonObject), getSchema()));
    }
    
    private static byte[] serialize(GenericRecord avroMessage, Schema schema) {
        
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            Encoder encoder = EncoderFactory.get().binaryEncoder(os, null); 

            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema); 
            writer.write(avroMessage, encoder); 
            encoder.flush(); 
            return os.toByteArray(); 
            
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    
    private GenericRecord toSingleAvroRecord(JsonObject jsonObject) {
        return jsonObjectToAvroWriter.apply(jsonObject);
    }
    
    public String toJsonObject(GenericRecord avroRecord) {
        // NOT YET IMPLEMENTED 
        return null;
    }
    
    
    @Override
    public String toString() {
        StringWriter stringWriter = new StringWriter();
        ImmutableMap<String, Boolean> config =ImmutableMap.of(JsonGenerator.PRETTY_PRINTING, true);
        JsonWriterFactory writerFactory = Json.createWriterFactory(config);
        JsonWriter jsonWriter = writerFactory.createWriter(stringWriter);
        jsonWriter.write(jsonSchema);
        jsonWriter.close();

        return stringWriter.toString();
    }
    
    
    
    public static JsonAvroEntityMapper createrMapper(JsonObject jsonRecordSchema) throws SchemaException {
        return createrRecordMapper(null, jsonRecordSchema);
    }
 
    
    private static JsonAvroEntityMapper createrRecordMapper(String parentnamespace, JsonObject jsonRecordSchema) throws SchemaException {
         
        final String namespace = jsonRecordSchema.containsKey("namespace") ? jsonRecordSchema.getString("namespace") : parentnamespace;
        final String name = jsonRecordSchema.containsKey("name") ? jsonRecordSchema.getString("name") : null;
        final String type = jsonRecordSchema.getString("type");

        if (!type.equals("record")) {
            throw new SchemaException("unsupported type " + type);
        }  
         
        
        AvroWriters avroWriters = AvroWriters.create();
        final JsonArray schemaFields = jsonRecordSchema.getJsonArray("fields");
        for (JsonValue schemaField : schemaFields) {
            if (schemaField.getValueType() == JsonValue.ValueType.OBJECT) {
                final JsonObject jsonObjectSchema = (JsonObject) schemaField;
                avroWriters = parseAndRegisterObject(namespace, jsonObjectSchema, avroWriters);
            } else {
                throw new SchemaException("unexpected value " + schemaField);
            }
        }
        
        return new JsonAvroEntityMapper(namespace,
                                  name, 
                                  jsonRecordSchema, 
                                  new JsonObjectToAvroRecordWriter(jsonRecordSchema, avroWriters));
    }
     
  
    
        
    
     
    private static AvroWriters parseAndRegisterObject(String parentnamespace, JsonObject jsonObjectSchema, AvroWriters avroWriters) throws SchemaException {
        
        if (jsonObjectSchema.containsKey("name")) {
            final String objectFieldname = jsonObjectSchema.getString("name");
            final JsonValue objectFieldType = jsonObjectSchema.get("type");
            final JsonValue.ValueType objectFieldTypeValue = objectFieldType.getValueType();

            
            // primitive Types
            if (objectFieldTypeValue == JsonValue.ValueType.STRING) {
                return avroWriters.withWriter(objectFieldname, newpPimitiveWriter(objectFieldname, jsonObjectSchema.getString("type")));
                
            // union    
            } else if (objectFieldTypeValue == JsonValue.ValueType.ARRAY) {
                final JsonArray array = (JsonArray) objectFieldType; 
                
                if ((array.size() == 2)) {
                    JsonValue val = array.get(0);
                    if ((val.getValueType() == JsonValue.ValueType.STRING) && ((JsonString) val).getString().equals("null")) {
                        val = array.get(1);
                    }
                    
                    // optional record 
                    if (val.getValueType() == JsonValue.ValueType.OBJECT) {
                        final JsonAvroEntityMapper subObjectMapper = createrRecordMapper(parentnamespace, (JsonObject) val); 
                        return avroWriters.withWriter(objectFieldname, (jsonValue, avroRecord) -> avroRecord.put(objectFieldname, (subObjectMapper.toSingleAvroRecord(((JsonObject) jsonValue)))));

                    // optional primitives
                    } else if (val.getValueType() == JsonValue.ValueType.STRING) {
                        return avroWriters.withWriter(objectFieldname, newpPimitiveWriter(objectFieldname, ((JsonString) val).getString()));
                        
                    } else {
                        throw new SchemaException("unsupported union type " + jsonObjectSchema);
                    }
                
                // array size != 2
                } else {
                    throw new SchemaException("unsupported union type " + jsonObjectSchema);
                }
            
                
            } else if (objectFieldTypeValue == JsonValue.ValueType.OBJECT) {
                final JsonObject obj = (JsonObject) objectFieldType;
                final String type = obj.getString("type");
                
                // enum    
                if (type.equals("enum")) {
                    return avroWriters.withWriter(objectFieldname, (jsonValue, avroRecord) -> avroRecord.put(objectFieldname, ((JsonString) jsonValue).getString()));

                // object    
                } else if (type.equals("record")) {
                    JsonAvroEntityMapper subObjectMapper = createrRecordMapper(parentnamespace, obj); 
                    return avroWriters.withWriter(objectFieldname, (jsonValue, avroRecord) -> avroRecord.put(objectFieldname, subObjectMapper.toSingleAvroRecord(((JsonObject) jsonValue))));
                    
                } else {
                    throw new SchemaException("unknown type " + jsonObjectSchema);
                }
                
            } else {
                throw new SchemaException("unknown type " + jsonObjectSchema);
            }
            
        } else {
            throw new SchemaException("unsupported schema " + jsonObjectSchema); 
        }
    }
     
    
    
    
    private static BiConsumer<JsonValue, GenericRecord> newpPimitiveWriter(String fieldname, String fieldtype) {
        
        if (fieldtype.equals("string")) {
            return (jsonValue, avroRecord) -> avroRecord.put(fieldname, ((JsonString) jsonValue).getString());
            
        } else if (fieldtype.equals("boolean")) {
            return (jsonValue, avroRecord) ->  avroRecord.put(fieldname, jsonValue.equals(JsonValue.TRUE));
            
        } else if (fieldtype.equals("int")) {
            return (jsonValue, avroRecord) -> avroRecord.put(fieldname, ((JsonNumber) jsonValue).intValue());

        } else if (fieldtype.equals("long")) {
            return (jsonValue, avroRecord) -> avroRecord.put(fieldname, ((JsonNumber) jsonValue).longValue());

        } else if (fieldtype.equals("float")) {
            return (jsonValue, avroRecord) -> avroRecord.put(fieldname, (float) ((JsonNumber) jsonValue).doubleValue());

        } else if (fieldtype.equals("double")) {
            return (jsonValue, avroRecord) -> avroRecord.put(fieldname, ((JsonNumber) jsonValue).doubleValue());

        } else if (fieldtype.equals("bytes")) {
            throw new SchemaException(fieldname + " is not yet supported on purpose");
            
        } else {
            throw new SchemaException("unknown type " + fieldtype);
        }
    }
   

    
    private static class JsonObjectToAvroRecordWriter implements Function<JsonValue, GenericRecord> {
        private final Schema schema;
        private final AvroWriters avroWriters;
       
        public JsonObjectToAvroRecordWriter(JsonObject schemaString, AvroWriters avroWriters) {
            this.schema = new Schema.Parser().parse(schemaString.toString());
            this.avroWriters = avroWriters;
        }
      
      
        @Override
        public GenericRecord apply(JsonValue jsonValue) {
            final GenericRecord avroRecord = new GenericData.Record(schema);

            for (Entry<String, JsonValue> nameValuePair : ((JsonObject) jsonValue).entrySet()) {
                final JsonValue value = nameValuePair.getValue();
                if (!value.equals(JsonValue.NULL)) {
                    avroWriters.get(nameValuePair.getKey()).accept(value, avroRecord);
                }
            }
          
            return avroRecord;
        }
    } 
  
  


    private static final class AvroWriters extends AbstractMap<String, BiConsumer<JsonValue, GenericRecord>> {
        private static final BiConsumer<JsonValue, GenericRecord> EMPTY_WRITER = (jsonValue, record) -> { };
        private final ImmutableMap<String, BiConsumer<JsonValue, GenericRecord>> writers;
        
        private AvroWriters(ImmutableMap<String, BiConsumer<JsonValue, GenericRecord>> writers) {
            this.writers = writers;
        }

        public static AvroWriters create() {
            return new AvroWriters(ImmutableMap.of());
        }
        
        public AvroWriters withWriter(String name, BiConsumer<JsonValue, GenericRecord> writer) {
            return new AvroWriters(ImmutableMap.<String, BiConsumer<JsonValue, GenericRecord>>builder()
                                               .putAll(writers)
                                               .put(name, writer).build());
        }
        
        public BiConsumer<JsonValue, GenericRecord> get(String name) {
            BiConsumer<JsonValue, GenericRecord> writer = writers.get(name);
            return (writer == null) ? EMPTY_WRITER : writer;
        }
        
        @Override
        public ImmutableSet<Entry<String, BiConsumer<JsonValue, GenericRecord>>> entrySet() {  
            return writers.entrySet();
        }
    }
}