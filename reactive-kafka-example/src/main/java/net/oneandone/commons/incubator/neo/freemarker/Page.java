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
package net.oneandone.commons.incubator.neo.freemarker;


import com.google.common.collect.ImmutableMap;


public class Page {    
    
    private final String path;
    private final ImmutableMap<String, Object> data;
    
    
    
    public Page(String path) {
        this(path, ImmutableMap.of());
    }
    
    protected Page(String path, ImmutableMap<String, Object> data) {
        this.path = path;     
        this.data = data;
    }
    
    public String getPath() {
        return path;
    }
    
    public ImmutableMap<String, Object> getModelMap() {
        return data;
    }
    
    public Page withModelData(String name, Object value) {
        return new Page(path, ImmutableMap.<String, Object>builder().putAll(data).put(name, value).build());
    }
  
    public Page withModelData(ImmutableMap<String, Object> addtionaldata) {
        return new Page(path, ImmutableMap.<String, Object>builder().putAll(data).putAll(addtionaldata).build());
    }

    @Override
    public String toString() {
        return getPath();
    }
}
