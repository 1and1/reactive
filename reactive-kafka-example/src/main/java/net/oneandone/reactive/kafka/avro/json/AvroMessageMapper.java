/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.reactive.kafka.avro.json;


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
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.mortbay.log.Log;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import net.oneandone.reactive.utils.Pair;





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
        return toJson(avroMessage.getGenericRecord());
    }
    
    
    public JsonObject toJson(GenericRecord genericRecord) {
        return avroRecordToJsonObjectWriter.apply(genericRecord);
    }
    
    @Override
    public int hashCode() {
        return toString().hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
        return (other != null) && (other instanceof AvroMessageMapper) && ((AvroMessageMapper) other).toString().equals(this.toString());
    }
    
    @Override
    public String toString() {
        final StringWriter stringWriter = new StringWriter();
        final ImmutableMap<String, Boolean> config = ImmutableMap.of(JsonGenerator.PRETTY_PRINTING, true);
        final JsonWriterFactory writerFactory = Json.createWriterFactory(config);
        final JsonWriter jsonWriter = writerFactory.createWriter(stringWriter);
        jsonWriter.write(jsonSchema);
        jsonWriter.close();

        return stringWriter.toString();
    }
    
    
    
    public static Pair<AvroMessageMapper, SchemaInfo> createrMapper(SchemaInfo schemaInfo) throws SchemaException {
        try {
            AvroMessageMapper mapper = createrRecordMapper(toJsonObject(schemaInfo.getSchema()));
            return Pair.of(mapper, null);
        } catch (SchemaException se) {
            Log.warn("invalid schema " + schemaInfo.getSchema(), se);
            return Pair.of(null, new SchemaInfo(schemaInfo.getSource(), schemaInfo.getLastModifiedTime(), schemaInfo.getSchema(), se.getMessage()));
        }
    }
 
    private static JsonObject toJsonObject(Schema schema) {
        return Json.createReader(new ByteArrayInputStream(schema.toString().getBytes(Charsets.UTF_8)))
                   .readObject();
    }
    
    private static AvroMessageMapper createrRecordMapper(JsonObject jsonRecordSchema) throws SchemaException {
        final String type = jsonRecordSchema.getString("type");
        if (!type.equals("record")) {
            throw new SchemaException("unsupported record type " + jsonRecordSchema, type);
        }  

        Writers writers = Writers.create();
        final JsonArray schemaFields = jsonRecordSchema.getJsonArray("fields");
        for (JsonValue schemaField : schemaFields) {
            if (schemaField.getValueType() == JsonValue.ValueType.OBJECT) {
                writers = withWriters(addNamespaceIfNotPresent((JsonObject) schemaField, jsonRecordSchema), writers);
            } else {
                throw new SchemaException("unexpected value " + schemaField, type);
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
        final JsonObjectBuilder builder = Json.createObjectBuilder();
        
        for (Entry<String, JsonValue> nameValuePair : reader.readObject().entrySet()) {
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
                    final JsonValue val;
                    if ((array.get(0).getValueType() == JsonValue.ValueType.STRING) && ((JsonString) array.get(0)).getString().equals("null")) {
                        val = array.get(1);
                    } else if ((array.get(1).getValueType() == JsonValue.ValueType.STRING) && ((JsonString) array.get(1)).getString().equals("null")) {
                        val = array.get(0);
                    } else {
                        val = array.get(0);
                    }

                    
                    // optional record  
                    if (val.getValueType() == JsonValue.ValueType.OBJECT) {
                        final AvroMessageMapper subObjectMapper = createrRecordMapper(addNamespaceIfNotPresent((JsonObject) val, jsonObjectSchema)); 
                        return avroWriters.withAvroWriter(objectFieldname, (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.START_OBJECT) avroRecord.put(objectFieldname, (subObjectMapper.toAvroMessage(jsonParser).getGenericRecord())); })
                                          .withJsonWriter((avroRecord, jsonBuilder) -> { if (avroRecord.get(objectFieldname) != null)  jsonBuilder.add(objectFieldname, subObjectMapper.toJson((GenericRecord) avroRecord.get(objectFieldname))); });

                        
                    // optional primitives
                    } else if (val.getValueType() == JsonValue.ValueType.STRING) { 
                        return avroWriters.withAvroWriter(objectFieldname, newPimitiveAvroWriter(objectFieldname, ((JsonString) val).getString()))
                                          .withJsonWriter(newPimitiveJsonWriter(objectFieldname, ((JsonString) val).getString()));
                                                  
                        
                    } else {
                        throw new SchemaException("unsupported union type " + jsonObjectSchema);
                    }
                
                // array size != 2
                } else {
                    throw new SchemaException("unsupported union type " + array);
                }
            
                
            } else if (objectFieldTypeValue == JsonValue.ValueType.OBJECT) {
                final JsonObject obj = (JsonObject) objectFieldType;
                final String type = obj.getString("type");
                
                // enum    
                if (type.equals("enum")) {
                    return avroWriters.withAvroWriter(objectFieldname, (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.VALUE_STRING) avroRecord.put(objectFieldname, jsonParser.getString()); })
                                      .withJsonWriter(newPimitiveJsonWriter(objectFieldname, type)); 

                    
                // object    
                } else if (type.equals("record")) {
                    final AvroMessageMapper subObjectMapper = createrRecordMapper(addNamespaceIfNotPresent(obj, jsonObjectSchema)); 
                    return avroWriters.withAvroWriter(objectFieldname, (jsonParser, avroRecord) -> { if (jsonParser.next() == Event.START_OBJECT) avroRecord.put(objectFieldname, subObjectMapper.toAvroMessage(jsonParser).getGenericRecord()); })
                                      .withJsonWriter((avroRecord, jsonBuilder) ->  { if (avroRecord.get(objectFieldname) != null) jsonBuilder.add(objectFieldname, subObjectMapper.toJson(AvroMessage.from((GenericRecord) avroRecord.get(objectFieldname)))); });
                    
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
            
            case "enum":
                return (avroRecord, jsonBuilder) -> { if (avroRecord.get(fieldname) != null) jsonBuilder.add(fieldname, ((EnumSymbol) avroRecord.get(fieldname)).toString()); };
                
            case "string":
                return (avroRecord, jsonBuilder) -> { if (avroRecord.get(fieldname) != null) jsonBuilder.add(fieldname, ((Utf8) avroRecord.get(fieldname)).toString()); };
    
            case "boolean":
                return (avroRecord, jsonBuilder) -> { if (avroRecord.get(fieldname) != null) jsonBuilder.add(fieldname, (Boolean) avroRecord.get(fieldname)); };
    
            case "int":
                return (avroRecord, jsonBuilder) -> { if (avroRecord.get(fieldname) != null) jsonBuilder.add(fieldname, (int) avroRecord.get(fieldname)); };
    
            case "long":
                return (avroRecord, jsonBuilder) -> { if (avroRecord.get(fieldname) != null) jsonBuilder.add(fieldname, (long) avroRecord.get(fieldname)); };
    
            case "float":
                return (avroRecord, jsonBuilder) -> { if (avroRecord.get(fieldname) != null) jsonBuilder.add(fieldname, (float) avroRecord.get(fieldname)); };
                
            case "double":
                return (avroRecord, jsonBuilder) -> { if (avroRecord.get(fieldname) != null) jsonBuilder.add(fieldname, (double) avroRecord.get(fieldname)); };
    
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