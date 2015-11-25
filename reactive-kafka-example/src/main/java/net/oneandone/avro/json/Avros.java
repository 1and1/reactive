package net.oneandone.avro.json;


import java.io.ByteArrayOutputStream;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;




public class Avros {
        
    private Avros() { }
    
    public static byte[] serializeAvroMessage(GenericRecord avroMessage, Schema schema) {
        
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final Encoder encoder = EncoderFactory.get().binaryEncoder(os, null); 

            final GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema); 
            writer.write(avroMessage, encoder); 
            encoder.flush(); 
            return os.toByteArray(); 
            
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}