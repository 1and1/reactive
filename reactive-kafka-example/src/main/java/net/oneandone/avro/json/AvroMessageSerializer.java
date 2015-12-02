package net.oneandone.avro.json;


import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;



public class AvroMessageSerializer {

    private AvroMessageSerializer() { }
    
    public static ImmutableList<byte[]> serialize(Schema schema, ImmutableList<GenericRecord> avroMessages) {
        
        List<byte[]> serializedRecords = com.google.common.collect.Lists.newArrayList();
        
        for (GenericRecord avroMessage : avroMessages) {
            try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                final Encoder encoder = EncoderFactory.get().binaryEncoder(os, null); 

                final GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema); 
                writer.write(avroMessage, encoder); 
                encoder.flush(); 
                
                serializedRecords.add(withHeader(schema, os.toByteArray())); 
                
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }            
        
        return ImmutableList.copyOf(serializedRecords);
    }
    
    
    private static byte[] withHeader(Schema schema, byte[] msg) {
        byte[] header = (schema.getNamespace() + "#" + schema.getName()).getBytes(Charsets.UTF_8);
        byte[] lengthHeader = ByteBuffer.allocate(4).putInt(header.length).array();
         
        byte[] newBytes = new byte[4 + header.length + msg.length];
        System.arraycopy(lengthHeader, 0, newBytes, 0, lengthHeader.length);
        System.arraycopy(header, 0, newBytes, lengthHeader.length, header.length);
        System.arraycopy(msg, 0, newBytes, 4 + header.length, msg.length);
        
        return newBytes;
    }
    
    
    public static GenericRecord deserialize(byte[] serialized, 
                                            JsonAvroMapperRegistry jsonAvroMapperRegistry,
                                            Schema readerSchema) {
        ByteBuffer buffer = ByteBuffer.wrap(serialized);
        int headerLength = buffer.getInt();
        
        byte[] headerBytes = new byte[headerLength];
        buffer.get(headerBytes);
        ImmutableList<String> namespaceName = ImmutableList.copyOf(Splitter.on('#').splitToList(new String(headerBytes, Charsets.UTF_8)));
        Schema writerSchema = jsonAvroMapperRegistry.getJsonToAvroMapper(namespaceName.get(0), namespaceName.get(1)).get().getSchema();
        
        byte[] msg = new byte[buffer.remaining()];
        buffer.get(msg);
        
        return deserializeAvroMessage(msg, writerSchema, readerSchema);   
    }
    
    private static GenericRecord deserializeAvroMessage(byte[] bytes, Schema writerSchema, Schema readerSchema) {
        
        try {
            DatumReader<GenericRecord> reader = (readerSchema == null) ? new SpecificDatumReader<GenericRecord>(writerSchema)
                                                                       : new SpecificDatumReader<GenericRecord>(writerSchema, readerSchema) ;
            Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            return reader.read(null, decoder);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}
