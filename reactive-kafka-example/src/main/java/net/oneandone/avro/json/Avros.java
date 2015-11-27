package net.oneandone.avro.json;


import java.io.ByteArrayOutputStream;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;




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
    
    
    public static GenericRecord deserializeAvroMessage(byte[] bytes, Schema schema) {
        try {
            DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            return reader.read(null, decoder);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}