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


import java.io.ByteArrayOutputStream;

import java.io.IOException;

import java.nio.ByteBuffer;

import javax.ws.rs.core.MediaType;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;




public class AvroMessage {
    private static final Logger LOG = LoggerFactory.getLogger(AvroMessage.class);
    

    private final GenericRecord avroMessage;
    
    private AvroMessage(GenericRecord avroMessage) {
        this.avroMessage = avroMessage;
    }
    
    
    public GenericRecord getGenericRecord() {
        return avroMessage;
    }

    public Schema getSchema() {
        return avroMessage.getSchema();
    }
    
    public MediaType getMimeType() {
        return MediaType.valueOf("application/vnd." + Joiner.on(".").join(getSchema().getNamespace(), getSchema().getName().replace("_v", "-v")) + "+json");
    }
    
    public byte[] getBinaryData() throws AvroSerializationException {
        return serialize(avroMessage);
    }
    
    @Override
    public String toString() {
        return avroMessage.toString();
    }
    
    static AvroMessage from(GenericRecord avroRecord) {
        return new AvroMessage(avroRecord);
    }
    
    
    static AvroMessage from(byte[] serialized, ImmutableMap<String, Schema> schemaRegistry, ImmutableList<Schema> readerSchemas) {
        ByteBuffer buffer = ByteBuffer.wrap(serialized);
        int headerLength = buffer.getInt();
        
        byte[] headerBytes = new byte[headerLength];
        buffer.get(headerBytes);
        ImmutableList<String> namespaceName = ImmutableList.copyOf(Splitter.on('#').splitToList(new String(headerBytes, Charsets.UTF_8)));
        Schema writerSchema = schemaRegistry.get(namespaceName.get(0) + "." + namespaceName.get(1));
        
        byte[] msg = new byte[buffer.remaining()];
        buffer.get(msg);
        
        return new AvroMessage(deserializeAvroMessage(msg, writerSchema, readerSchemas));
    }
    
    
    private static GenericRecord deserializeAvroMessage(byte[] bytes, Schema writerSchema, ImmutableList<Schema> readerSchemas) {
        
        for (Schema readerSchema : readerSchemas) {
            try {
                return deserializeAvroMessage(bytes, writerSchema, readerSchema);
            } catch (RuntimeException ignore) { }
        }
        
        return deserializeAvroMessage(bytes, writerSchema, (Schema) null);
    }


    private static GenericRecord deserializeAvroMessage(byte[] bytes, Schema writerSchema, Schema readerSchema) {
        try {
            DatumReader<GenericRecord> reader = (readerSchema == null) ? new SpecificDatumReader<GenericRecord>(writerSchema)
                                                                       : new SpecificDatumReader<GenericRecord>(writerSchema, readerSchema) ;
            Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            GenericRecord avroRecord = reader.read(null, decoder);
            return avroRecord;
        } catch (IOException ioe) {
            LOG.debug("error occured by deserialize avro record", ioe);
            throw new RuntimeException(ioe);
        } catch (AvroTypeException ae) {
            LOG.debug("error occured by  deserialize avro record", ae);
            throw ae;
        }
    }
    
    
    
    
    private static byte[] serialize(GenericRecord avroMessage) throws AvroSerializationException {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            final Encoder encoder = EncoderFactory.get().binaryEncoder(os, null); 

            final GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(avroMessage.getSchema()); 
            writer.write(avroMessage, encoder); 
            encoder.flush(); 
            
            return withHeader(avroMessage.getSchema(), os.toByteArray()); 
        } catch(IOException | RuntimeException rt) {
            throw new AvroSerializationException("serializing avro message failed (wrong schema type?)", avroMessage.getSchema().getFullName(), rt);
        }
    }
    
    
    private static byte[] withHeader(Schema schema, byte[] msg) {
        final byte[] header = (schema.getNamespace() + "#" + schema.getName()).getBytes(Charsets.UTF_8);
        final byte[] lengthHeader = ByteBuffer.allocate(4).putInt(header.length).array();
         
        final byte[] newBytes = new byte[4 + header.length + msg.length];
        System.arraycopy(lengthHeader, 0, newBytes, 0, lengthHeader.length);
        System.arraycopy(header, 0, newBytes, lengthHeader.length, header.length);
        System.arraycopy(msg, 0, newBytes, 4 + header.length, msg.length);
        
        return newBytes;
    }    
}
