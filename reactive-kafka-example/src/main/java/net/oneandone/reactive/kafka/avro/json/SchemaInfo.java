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

import java.util.Optional;

import org.apache.avro.Schema;

public class SchemaInfo {
    private String source;
    private Optional<String> error;
    private Schema schema;
    
    SchemaInfo(String source, Schema schema) {
        this(source, schema, null);
    }
    
    SchemaInfo(String source, Schema schema, String error) {
        this.source = source;
        this.error = Optional.ofNullable(error);
        this.schema = schema;
    }

    public String getSource() {
        return source;
    }

    public Optional<String> getError() {
        return error;
    }

    public Schema getSchema() {
        return schema;
    }
    
    @Override
    public String toString() {
        return "[" + source + "]\r\n" +
               schema.toString(true);
    }
    
    @Override
    public int hashCode() {
        return toString().hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
        return (other != null) && (other instanceof SchemaInfo) && (other.toString().equals(this.toString()));
    }
}