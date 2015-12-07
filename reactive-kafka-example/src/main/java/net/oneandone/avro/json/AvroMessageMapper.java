package net.oneandone.avro.json;


import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;





class AvroMessageMapper {
        
    private final Schema schema;
    private final JsonObject jsonSchema;
    private final JsonObjectToAvroRecordWriter jsonObjectToAvroWriter;
    private final AvroRecordToJsonObjectWriter avroRecordToJsonObjectWriter; 
    private final String mimeType;
    
    
    private AvroMessageMapper(JsonObject jsonSchema,
                           JsonObjectToAvroRecordWriter jsonObjectToAvroMapper,
                           AvroRecordToJsonObjectWriter avroRecordToJsonObjectWriter) {
        this.jsonSchema = jsonSchema;
        this.schema = new Schema.Parser().parse(jsonSchema.toString());
        this.jsonObjectToAvroWriter = jsonObjectToAvroMapper;
        this.avroRecordToJsonObjectWriter = avroRecordToJsonObjectWriter;
        
        this.mimeType  = "application/vnd." + Joiner.on(".").join(schema.getNamespace(), schema.getName().replace("_v", "-v")) + "+json";
    }
  

    public String getMimeType() { 
        return mimeType;
    }

    
    public Schema getSchema() {
        return schema;
    }
    
  
    public ImmutableList<AvroMessage> toAvroMessages(JsonParser jsonParser) {
        
        // check initial state
        if (jsonParser.next() != Event.START_ARRAY) {
            throw new IllegalStateException("START_ARRAY event excepted");
        }

        
        final List<AvroMessage> avroMessages = Lists.newArrayList();
        
        
        while (jsonParser.hasNext()) {
            
            switch (jsonParser.next()) {

            case END_ARRAY:
                return ImmutableList.copyOf(avroMessages);

            case START_OBJECT:
                avroMessages.add(toAvroMessage(jsonParser));
                break;
                
            default:
            }
        }

        throw new IllegalStateException("END_ARRAY event is missing");
    }
      
    
    public AvroMessage toAvroMessage(JsonParser jsonParser) {
        return AvroMessage.from(jsonObjectToAvroWriter.apply(jsonParser));
    }
    
    
    public JsonObject toJson(AvroMessage avroMessage) {
        return avroRecordToJsonObjectWriter.apply(avroMessage.getGenericRecord());
    }
    
    
    @Override
    public String toString() {
        final StringWriter stringWriter = new StringWriter();
        final ImmutableMap<String, Boolean> config =ImmutableMap.of(JsonGenerator.PRETTY_PRINTING, true);
        final JsonWriterFactory writerFactory = Json.createWriterFactory(config);
        final JsonWriter jsonWriter = writerFactory.createWriter(stringWriter);
        jsonWriter.write(jsonSchema);
        jsonWriter.close();

        return stringWriter.toString();
    }
    
    
    
    public static AvroMessageMapper createrMapper(JsonObject jsonRecordSchema) throws SchemaException {
        return createrRecordMapper(jsonRecordSchema);
    }
 
    
    
    private static AvroMessageMapper createrRecordMapper(JsonObject jsonRecordSchema) throws SchemaException {
         
        final String type = jsonRecordSchema.getString("type");
        if (!type.equals("record")) {
            throw new SchemaException("unsupported type " + type);
        }  

        
        Writers writers = Writers.create();
        final JsonArray schemaFields = jsonRecordSchema.getJsonArray("fields");
        for (JsonValue schemaField : schemaFields) {
            if (schemaField.getValueType() == JsonValue.ValueType.OBJECT) {
                writers = withWriters(addNamespaceIfNotPresent((JsonObject) schemaField, jsonRecordSchema), writers);
            } else {
                throw new SchemaException("unexpected value " + schemaField);
            }
        }
        
        return new AvroMessageMapper(jsonRecordSchema,
                                  new JsonObjectToAvroRecordWriter(jsonRecordSchema, writers),
                                  new AvroRecordToJsonObjectWriter(writers));
    }
     
  

    private static JsonObject addNamespaceIfNotPresent(JsonObject jsonObject, JsonObject parentJsonObject) {
        return (jsonObject.containsKey("namespace")) ? jsonObject : addAttribute(jsonObject, "namespace", parentJsonObject.getString("namespace"));
    }
     
    
    
    private static JsonObject addAttribute(JsonObject jsonObject, String name, String value) {
        
        final JsonReader reader = Json.createReader(new ByteArrayInputStream(jsonObject.toString().getBytes(Charsets.UTF_8)));
        JsonObject json =  reader.readObject();

        JsonObjectBuilder builder = Json.createObjectBuilder();
        
        for (Entry<String, JsonValue> nameValuePair : json.entrySet()) {
            builder.add(nameValuePair.getKey(), nameValuePair.getValue());
        }
        
        builder.add(name, value);
        
        return builder.build();
    }    
    
     
    private static Writers withWriters(JsonObject jsonObjectSchema, Writers avroWriters) throws SchemaException {
        
        if (jsonObjectSchema.containsKey("name")) {
            final String objectFieldname = jsonObjectSchema.getString("name");
            final JsonValue objectFieldType = jsonObjectSchema.get("type");
            final JsonValue.ValueType objectFieldTypeValue = objectFieldType.getValueType();

            
            // primitive Types
            if (objectFieldTypeValue == JsonValue.ValueType.STRING) {
                return avroWriters.withAvroWriter(objectFieldname, newPimitiveAvroWriter(objectFieldname, jsonObjectSchema.getString("type")))
                                  .withJsonWriter(newPimitiveJsonWriter(objectFieldname, jsonObjectSchema.getString("type")));
                
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
                        final AvroMessageMapper subObjectMapper = createrRecordMapper(addNamespaceIfNotPresent((JsonObject) val, jsonObjectSchema)); 
                        return avroWriters.withAvroWriter(objectFieldname, (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.START_OBJECT) avroRecord.put(objectFieldname, (subObjectMapper.toAvroMessage(jsonParser).getGenericRecord())); });

                    // optional primitives
                    } else if (val.getValueType() == JsonValue.ValueType.STRING) { 
                        return avroWriters.withAvroWriter(objectFieldname, newPimitiveAvroWriter(objectFieldname, ((JsonString) val).getString()));
                        
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
                    return avroWriters.withAvroWriter(objectFieldname, (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.VALUE_STRING) avroRecord.put(objectFieldname, jsonParser.getString()); });

                // object    
                } else if (type.equals("record")) {
                    final AvroMessageMapper subObjectMapper = createrRecordMapper(addNamespaceIfNotPresent(obj, jsonObjectSchema)); 
                    return avroWriters.withAvroWriter(objectFieldname, (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.START_OBJECT) avroRecord.put(objectFieldname, subObjectMapper.toAvroMessage(jsonParser).getGenericRecord()); })
                                      .withJsonWriter((avroRecord, jsonBuilder) ->  jsonBuilder.add(objectFieldname, subObjectMapper.toJson(AvroMessage.from((GenericRecord) avroRecord.get(objectFieldname)))));
                    
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
     
    
    
    
    private static BiConsumer<JsonParser, GenericRecord> newPimitiveAvroWriter(String fieldname, String fieldtype) {
        
        switch (fieldtype) {
            
            case "string":
                return (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.VALUE_STRING) avroRecord.put(fieldname, jsonParser.getString()); };
    
            case "boolean":
                return (jsonParser, avroRecord) -> avroRecord.put(fieldname, jsonParser.next() == Event.VALUE_TRUE);
    
            case "int":
                return (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.VALUE_NUMBER) avroRecord.put(fieldname, jsonParser.getInt()); };
    
            case "long":
                return (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.VALUE_NUMBER) avroRecord.put(fieldname, jsonParser.getLong()); };
    
            case "float":
                return (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.VALUE_NUMBER) avroRecord.put(fieldname, jsonParser.getBigDecimal().floatValue()); };
                
            case "double":
                return (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.VALUE_NUMBER) avroRecord.put(fieldname, jsonParser.getBigDecimal().doubleValue()); };
    
            case "bytes":
                throw new SchemaException(fieldname + " is not yet supported on purpose");
    
            case "fixed":
                throw new SchemaException(fieldname + " is not yet supported on purpose");
    
            default:
                throw new SchemaException("unknown type " + fieldtype);
        }
    }
    
    
    
    private static BiConsumer<GenericRecord, JsonObjectBuilder> newPimitiveJsonWriter(String fieldname, String fieldtype) {
        
        switch (fieldtype) {
            
            case "string":
                return (avroRecord, jsonBuilder) -> jsonBuilder.add(fieldname, new String(((Utf8) avroRecord.get(fieldname)).getBytes(), Charsets.UTF_8));
    
            case "boolean":
                return (avroRecord, jsonBuilder) -> jsonBuilder.add(fieldname, (Boolean) avroRecord.get(fieldname)); 
    
            case "int":
                return (avroRecord, jsonBuilder) -> jsonBuilder.add(fieldname, (int) avroRecord.get(fieldname));
    
            case "long":
                return (avroRecord, jsonBuilder) -> jsonBuilder.add(fieldname, (long) avroRecord.get(fieldname));
    
            case "float":
                return (avroRecord, jsonBuilder) -> jsonBuilder.add(fieldname, (float) avroRecord.get(fieldname));
                
            case "double":
                return (avroRecord, jsonBuilder) -> jsonBuilder.add(fieldname, (double) avroRecord.get(fieldname));
    
            case "bytes":
                throw new SchemaException(fieldname + " is not yet supported on purpose");
    
            case "fixed":
                throw new SchemaException(fieldname + " is not yet supported on purpose");
    
            default:
                throw new SchemaException("unknown type " + fieldtype);
        }
    }
   
    

    private static class AvroRecordToJsonObjectWriter implements Function<GenericRecord, JsonObject> {
        
        private final Writers writers;
       
        public AvroRecordToJsonObjectWriter(Writers writers) {
            this.writers = writers;
        }
        
        public JsonObject apply(GenericRecord avroMessage) {
            
            JsonObjectBuilder jsonBuilder = Json.createObjectBuilder();
            
            for (BiConsumer<GenericRecord, JsonObjectBuilder> jsonWriter : writers.getJsonWriters()) {
                jsonWriter.accept(avroMessage, jsonBuilder);
            }
            
            return jsonBuilder.build();
        };
    }

    
    
    private static class JsonObjectToAvroRecordWriter implements Function<JsonParser, GenericRecord> {
        private final Schema schema;
        private final Writers writers;
       
        public JsonObjectToAvroRecordWriter(JsonObject schemaString, Writers writers) {
            this.schema = new Schema.Parser().parse(schemaString.toString());
            this.writers = writers;
        }
      
      
        @Override
        public GenericRecord apply(JsonParser jsonParser) {
            final GenericRecord avroRecord = new GenericData.Record(schema);
            
            while (jsonParser.hasNext()) {
                
                switch (jsonParser.next()) {
                
                    case KEY_NAME:
                        writers.getAvroWriter(jsonParser.getString())     // get the registered writer (or EMPTY writer)
                               .accept(jsonParser, avroRecord);           // write to avro record 
                        break;
    
                    case END_OBJECT:
                        return avroRecord;
    
                    default:
                }
            }

            throw new IllegalStateException("END_OBJECT event is missing");
        }
    } 
  
  


    
    private static final class Writers {
        
        private static final BiConsumer<JsonParser, GenericRecord> EMPTY_AVRO_WRITER = (jsonValue, record) -> { };
        private final ImmutableMap<String, BiConsumer<JsonParser, GenericRecord>> avroWriters;
       
        private final ImmutableList<BiConsumer<GenericRecord, JsonObjectBuilder>> jsonWriters;
        
        private Writers(ImmutableMap<String, BiConsumer<JsonParser, GenericRecord>> avroWriters,
                        ImmutableList<BiConsumer<GenericRecord, JsonObjectBuilder>> jsonWriters) {
            this.avroWriters = avroWriters;
            this.jsonWriters = jsonWriters;
        }

        public static Writers create() {
            return new Writers(ImmutableMap.of(), ImmutableList.of());
        }
        
        public Writers withAvroWriter(String name, BiConsumer<JsonParser, GenericRecord> avroWriter) {
            return new Writers(ImmutableMap.<String, BiConsumer<JsonParser, GenericRecord>>builder()
                                           .putAll(avroWriters)
                                           .put(name, avroWriter).build(),
                               jsonWriters);
        }
        
        public Writers withJsonWriter(BiConsumer<GenericRecord, JsonObjectBuilder> jsonWriter) {
            return new Writers(avroWriters,
                               ImmutableList.<BiConsumer<GenericRecord, JsonObjectBuilder>>builder()
                                           .addAll(jsonWriters)
                                           .add(jsonWriter).build());
        }
        
        public BiConsumer<JsonParser, GenericRecord> getAvroWriter(String name) {
            final BiConsumer<JsonParser, GenericRecord> avroWriter = avroWriters.get(name);
            return (avroWriter == null) ? EMPTY_AVRO_WRITER : avroWriter;
        }
        
        public ImmutableList<BiConsumer<GenericRecord, JsonObjectBuilder>> getJsonWriters() {
            return jsonWriters;
        }
    }
}