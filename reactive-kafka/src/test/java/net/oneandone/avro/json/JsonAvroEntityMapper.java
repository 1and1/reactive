package net.oneandone.avro.json;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.AbstractMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import com.google.common.base.Charsets;
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
        JsonParser jsonParser = Json.createParser(new ByteArrayInputStream(jsonObject.toString().getBytes(Charsets.UTF_8)));
        
        // check initial state
        if (jsonParser.next() != Event.START_OBJECT) {
            throw new IllegalStateException("START_OBJECT event excepted");
        }
        
        return ImmutableList.of(toSingleAvroRecord(jsonParser));
    }
    
    
    @Override
    public ImmutableList<byte[]> toAvroBinaryRecord(JsonObject jsonObject) {
        return ImmutableList.copyOf(toAvroRecord(jsonObject).stream()
                                                            .map(record -> serialize(record, getSchema()))
                                                            .collect(Collectors.toList()));
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
    
    private GenericRecord toSingleAvroRecord(JsonParser jsonParser) {
        return jsonObjectToAvroWriter.apply(jsonParser);
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
                        return avroWriters.withWriter(objectFieldname, (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.START_OBJECT) avroRecord.put(objectFieldname, (subObjectMapper.toSingleAvroRecord(jsonParser))); });

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
                    return avroWriters.withWriter(objectFieldname, (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.VALUE_STRING) avroRecord.put(objectFieldname, jsonParser.getString()); });

                // object    
                } else if (type.equals("record")) {
                    JsonAvroEntityMapper subObjectMapper = createrRecordMapper(parentnamespace, obj); 
                    return avroWriters.withWriter(objectFieldname, (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.START_OBJECT) avroRecord.put(objectFieldname, subObjectMapper.toSingleAvroRecord(jsonParser)); });
                    
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
     
    
    
    
    private static BiConsumer<JsonParser, GenericRecord> newpPimitiveWriter(String fieldname, String fieldtype) {
        
        if (fieldtype.equals("string")) {
            return (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.VALUE_STRING) avroRecord.put(fieldname, jsonParser.getString()); };
            
        } else if (fieldtype.equals("boolean")) {
            return (jsonParser, avroRecord) -> avroRecord.put(fieldname, jsonParser.next() == Event.VALUE_TRUE);
            
        } else if (fieldtype.equals("int")) {
            return (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.VALUE_NUMBER) avroRecord.put(fieldname, jsonParser.getInt()); };

        } else if (fieldtype.equals("long")) {
            return (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.VALUE_NUMBER) avroRecord.put(fieldname, jsonParser.getLong()); };

        } else if (fieldtype.equals("float")) {
            return (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.VALUE_NUMBER) avroRecord.put(fieldname, jsonParser.getBigDecimal().floatValue()); };

        } else if (fieldtype.equals("double")) {
            return (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.VALUE_NUMBER) avroRecord.put(fieldname, jsonParser.getBigDecimal().doubleValue()); };

        } else if (fieldtype.equals("bytes")) {
            throw new SchemaException(fieldname + " is not yet supported on purpose");
            
        } else {
            throw new SchemaException("unknown type " + fieldtype);
        }
    }
   

    
    private static class JsonObjectToAvroRecordWriter implements Function<JsonParser, GenericRecord> {
        private final Schema schema;
        private final AvroWriters avroWriters;
       
        public JsonObjectToAvroRecordWriter(JsonObject schemaString, AvroWriters avroWriters) {
            this.schema = new Schema.Parser().parse(schemaString.toString());
            this.avroWriters = avroWriters;
        }
      
      
        @Override
        public GenericRecord apply(JsonParser jsonParser) {

            final GenericRecord avroRecord = new GenericData.Record(schema);
            
            while (jsonParser.hasNext()) {
                switch (jsonParser.next()) {
                case KEY_NAME:
                    BiConsumer<JsonParser, GenericRecord> writer = avroWriters.get(jsonParser.getString());
                    writer.accept(jsonParser, avroRecord);
                    break;

                case END_OBJECT:
                    return avroRecord;

                default:
                }
            }

            throw new IllegalStateException("END_OBJECT event is missing");
        }
    } 
  
  


    private static final class AvroWriters extends AbstractMap<String, BiConsumer<JsonParser, GenericRecord>> {
        private static final BiConsumer<JsonParser, GenericRecord> EMPTY_WRITER = (jsonValue, record) -> { };
        private final ImmutableMap<String, BiConsumer<JsonParser, GenericRecord>> writers;
        
        private AvroWriters(ImmutableMap<String, BiConsumer<JsonParser, GenericRecord>> writers) {
            this.writers = writers;
        }

        public static AvroWriters create() {
            return new AvroWriters(ImmutableMap.of());
        }
        
        public AvroWriters withWriter(String name, BiConsumer<JsonParser, GenericRecord> writer) {
            return new AvroWriters(ImmutableMap.<String, BiConsumer<JsonParser, GenericRecord>>builder()
                                               .putAll(writers)
                                               .put(name, writer).build());
        }
        
        public BiConsumer<JsonParser, GenericRecord> get(String name) {
            BiConsumer<JsonParser, GenericRecord> writer = writers.get(name);
            return (writer == null) ? EMPTY_WRITER : writer;
        }
        
        @Override
        public ImmutableSet<Entry<String, BiConsumer<JsonParser, GenericRecord>>> entrySet() {  
            return writers.entrySet();
        }
    }
}