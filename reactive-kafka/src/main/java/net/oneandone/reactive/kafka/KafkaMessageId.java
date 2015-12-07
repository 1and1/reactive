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
package net.oneandone.reactive.kafka;


import java.util.Base64;

import java.util.Objects;
import java.util.Optional;

import com.google.common.base.Charsets;



public class KafkaMessageId implements Comparable<KafkaMessageId> {
    private static final boolean IS_BASE64_ENCODED = false;
    
    private final int partition;
    private final long offset;
    
    public KafkaMessageId(int partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }
    

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }
    
    
    @Override
    public int hashCode() {
        return Objects.hash(partition, offset);
    }

    @Override
    public boolean equals(Object other) {
        return (other != null) && 
               (other instanceof KafkaMessageId) && 
               (((KafkaMessageId) other).partition == (this.partition)) &&
               (((KafkaMessageId) other).offset == (this.offset));
    }
    
    @Override
    public int compareTo(KafkaMessageId other) {
        int i = ((Integer) other.partition).compareTo(this.partition);
        if (i == 0) {
            i = ((Long) other.offset).compareTo(this.offset);
        }
        return i;    
    }
    
    
    String asString() {
        return partition + "_" + offset;
    }
    
    @Override
    public String toString() {
        return encode(asString());
    }


    static KafkaMessageId fromString(String id) {
        final int idx = id.indexOf("_");
        return new KafkaMessageId(Integer.parseInt(id.substring(0, idx)), Long.parseLong(id.substring(idx + 1, id.length())));
    }

    
    public static KafkaMessageId valueOf(String id) {
        return fromString(decode(id)); 
    }
    

    public static KafkaMessageId valueOf(int partition, long offset) {
        return new KafkaMessageId(partition, offset);
    }

    

    
    public static Optional<Long> getOffSet(KafkaMessageIdList ids, int partition) {
        
        for (KafkaMessageId id : ids) {
            if (id.getPartition() == partition) {
                return Optional.of(id.getOffset());
            }
        }
        
        return Optional.empty();
    }

   
    static String decode(String s) {
        return IS_BASE64_ENCODED ? new String(Base64.getUrlDecoder().decode(s), Charsets.UTF_8) : s;
    }

    static String encode(String s) {
        return  IS_BASE64_ENCODED ? Base64.getUrlEncoder().encodeToString(s.getBytes(Charsets.UTF_8)) : s;
    }
}
