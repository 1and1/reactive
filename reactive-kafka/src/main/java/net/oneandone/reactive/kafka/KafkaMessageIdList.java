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

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class KafkaMessageIdList implements Iterable<KafkaMessageId> {

    private final ImmutableList<KafkaMessageId> ids;
    
    
    public KafkaMessageIdList() {
        this(ImmutableList.of());
    }

    private KafkaMessageIdList(ImmutableList<KafkaMessageId> ids) {
        this.ids = ids;
    }

    public boolean isEmpty() {
        return ids.isEmpty();
    }
    
    public int size() {
        return ids.size();
    }
    
    public KafkaMessageIdList merge(KafkaMessageIdList other) {
        return new KafkaMessageIdList(ImmutableList.<KafkaMessageId>builder().addAll(this.ids).addAll(other.ids).build());
    }


    public KafkaMessageIdList erase(KafkaMessageId id) {
        List<KafkaMessageId> idList = Lists.newArrayList(ids);
        idList.remove(id);
        return new KafkaMessageIdList(ImmutableList.copyOf(idList));
    }

    
  
    public KafkaMessageIdList replacePartitionOffset(int partition, long offset) {
        List<KafkaMessageId> newIdentifiers = Lists.newArrayList(ids);
        
        // remove if partition offset already exists
        for (KafkaMessageId id : ImmutableList.copyOf(ids)) {
            if (id.getPartition() == partition) {
                newIdentifiers.remove(id);
            }
        }

        // add new partition offset
        newIdentifiers.add(KafkaMessageId.valueOf(partition, offset));
        
        return new KafkaMessageIdList(ImmutableList.copyOf(newIdentifiers));
    }

    
    
    public boolean contains(KafkaMessageId id) {
        return ids.contains(id);
    }
    
    @Override
    public Iterator<KafkaMessageId> iterator() {
        return ids.iterator();
    }
    
    @Override
    public String toString() {
        return KafkaMessageId.encode(Joiner.on(",")
                                           .join(ids.stream()
                                                    .map(id -> id.asString())
                                                    .collect(Collectors.toList())));
    }

    
    public static KafkaMessageIdList of(KafkaMessageId... ids) {
        return new KafkaMessageIdList(ImmutableList.copyOf(ids));
    }
  
    
    public static KafkaMessageIdList valueOf(String ids) {
        ImmutableList<KafkaMessageId> idList;
        if (Strings.isNullOrEmpty(ids)) {
            idList = ImmutableList.of();
        } else {
            idList = ImmutableList.copyOf(Splitter.on(",")
                                                  .trimResults()    
                                                  .splitToList(KafkaMessageId.decode(ids))
                                                  .stream()
                                                  .map(id -> KafkaMessageId.fromString(id))
                                                  .collect(Collectors.toList()));
        }
        
        return new KafkaMessageIdList(idList);
    }

    
}
