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


public class SchemaException extends RuntimeException {

    private static final long serialVersionUID = -3894104697627836613L;

    private final String type;


    public SchemaException(String reason) {
        super(reason);
        this.type = null;
    } 
        
    public SchemaException(String reason, String type) {
        super(reason);
        this.type = type;
    } 
    
    public String getType() {
        return type;
    }
}