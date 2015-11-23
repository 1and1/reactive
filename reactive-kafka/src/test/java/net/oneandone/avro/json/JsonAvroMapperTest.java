package net.oneandone.avro.json;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.Instant;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.stream.JsonParser;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DataFileWriter.AppendWriteException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;




public class JsonAvroMapperTest {

    private static JsonAvroEntityMapper mapper;

    
    @BeforeClass
    public static void beforeClass() throws IOException {
        String schema = Resources.toString(Resources.getResource("schemas" + File.separator + "example.avsc"), 
                                           Charsets.UTF_8);

        
        JsonReader reader = Json.createReader(new ByteArrayInputStream(schema.getBytes(Charsets.UTF_8)));
        mapper = JsonAvroEntityMapper.createrMapper(reader.readObject());
    }

    

    @Test(expected=AppendWriteException.class)
    public void testMissingMandatoryField() throws Exception {
            MyEvent event = new MyEvent("3454503", "test@example.org", MyEvent.Operation.ADDED, "566");
        event.emailaddress = null;
        
        writeToFile(mapper.getSchema(), mapper.toAvroRecord(toJsonParser(event)));
    }

    
    @Test
    public void testValid() throws Exception {
        writeToFile(mapper.getSchema(), mapper.toAvroRecord(toJsonParser(new MyEvent("3454503", "test@example.org", MyEvent.Operation.ADDED, "455"))));
    }
    
    
    
    @Test
    public void testValidWithNullRecord() throws Exception {
        MyEvent event = new MyEvent("3454503", "test@example.org", MyEvent.Operation.ADDED, "455");
        event.context = null;
        writeToFile(mapper.getSchema(), mapper.toAvroRecord(toJsonParser(event)));
    }
    
   
    
    @Test(expected=AppendWriteException.class)
    public void testInValidEnumValue() throws Exception {
        MyEvent event = new MyEvent("3454503", "test@example.org", MyEvent.Operation.ILLEGAL, "455");
        event.context = null;
        writeToFile(mapper.getSchema(), mapper.toAvroRecord(toJsonParser(event)));
    }
    
    
    
    private void writeToFile(Schema schema, ImmutableList<GenericRecord> avroRecords) throws IOException {
        GenericRecord record =  avroRecords.get(0);
        
        File file = new File("test.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(record);
        dataFileWriter.close();
        
        System.out.println("file written to " + file.getAbsolutePath());
    }
    
    
    private JsonParser toJsonParser(Object jsonObject) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            objectMapper.writeValue(bos, jsonObject);
            bos.close();
            
            return Json.createParser(new ByteArrayInputStream(bos.toByteArray()));
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    
    
    @XmlRootElement
    public static class MyEvent {
        public static enum Operation { ADDED, REMOVED, UPDATED, ILLEGAL };

        public String timestamp = Instant.now().toString();
        public String id;
        public boolean valid = true;
        public int code = 99;
        public long ref = 3444349l;
        public float factor;
        public double size = 5.6;
        public String optionaldate;
        public String emailaddress;
        public Operation operation;
        public EventSource eventSource;
        public Context context = new Context("START-56-34-3-12-34-12-234-END");
        
        
        public MyEvent() { }
        
        public MyEvent(String id, String emailaddress, Operation operation, String sourceId) {
            this.id = id;
            this.emailaddress = emailaddress;
            this.operation = operation;
            this.eventSource = new EventSource(sourceId);
        }
        
        
        public static class EventSource {
            public String id;
            
            public EventSource() { }
            
            public EventSource(String id) {
                this.id = id;
            }
        }
        
        public static class Context {
            public String ctxid;
            
            public Context() { }
            
            public Context(String ctxid) {
                this.ctxid = ctxid;
            }
        }
    }
    
    
}