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
package net.oneandone.reactive.sse;

import java.util.Optional;



/**
 * Event (Server-Sent Event. refer http://dev.w3.org/html5/eventsource/) 
 *
 * @author grro
 */
public interface ServerSentEvent {
    
    
    /**
     * @return the comment or NULL
     */
    Optional<String> getComment();
    
    /**
     * @return the data ('data:' value)  or NULL
     */
    Optional<String> getData();
    
    
    /**
     * @return the event type ('event:' value) or NULL
     */
    Optional<String> getEvent();
  
    
    /**
     * @return the id ('id:' value) or NULL
     */
    Optional<String> getId();

    
    /**
     * @return the retry ('retry:' value) or NULL
     */
    Optional<Integer> getRetry();
    


    /**
     * @return the "wire" formated event according to Server-Sent Events spec
     */
    @Override
    String toString();
  
    
    /**
     * @return the wire formatted sse 
     */
    default String toWire() {
        StringBuilder sb = new StringBuilder();
        
        getComment().ifPresent(comment -> sb.append(": " + comment + "\r\n"));
        getId().ifPresent(id -> sb.append("id: " + id + "\r\n"));
        getEvent().ifPresent(event -> sb.append("event: " + event + "\r\n"));
        getData().ifPresent(data -> sb.append("data: " + data + "\r\n"));
        getRetry().ifPresent(retry -> sb.append("retry: " + retry + "\r\n"));
        sb.append("\r\n");
        
        return sb.toString();
    }
    
    
    
    /**
     * @return new event
     */
    public static SSEBuildableEvent newEvent() {
        return new SSEBuildableEvent(null, null, null, null, null);
    }
    
    
    
    
    /**
     * A buildable sse event
     */
    public static class SSEBuildableEvent implements ServerSentEvent {
        private final String id;
        private final String event;
        private final String data;
        private final String comment;
        private final Integer retry;


        private SSEBuildableEvent(String id, String event, String data, String comment, Integer retry) {
            this.id = id;
            this.event = event;
            this.data = data;
            this.comment = comment;
            this.retry = retry;
        }
        
        
        /**
         * @param data  the data to set 
         * @return the "cloned", updated event
         */
        public SSEBuildableEvent data(String data) {
            return new SSEBuildableEvent(this.id,
                                         this.event, 
                                         data, 
                                         this.comment, 
                                         this.retry); 
        }
        
        /**
         * @param id  the is to set
         * @return the "cloned", updated event
         */
        public SSEBuildableEvent id(String id) {
            return new SSEBuildableEvent(id, 
                                         this.event, 
                                         this.data, 
                                         this.comment, 
                                         this.retry); 
        }
        
        /**
         * @param comment  the comment to set
         * @return the "cloned", updated event
         */
        public SSEBuildableEvent comment(String comment) {
            return new SSEBuildableEvent(this.id, 
                                         this.event, 
                                         this.data, 
                                         comment, 
                                         this.retry); 
        }
        
        /**
         * @param event  the event to set
         * @return the "cloned", updated event
         */
        public SSEBuildableEvent event(String event) {
            return new SSEBuildableEvent(this.id, 
                                         event, 
                                         this.data, 
                                         this.comment,
                                         this.retry); 
        }
        
        /**
         * @param retry the retry time to set
         * @return the "cloned", updated event
         */
        public SSEBuildableEvent retry(Integer retry) {
            return new SSEBuildableEvent(this.id, 
                                         this.event, 
                                         this.data, 
                                         this.comment,  
                                         retry); 
        }

        @Override
        public Optional<String> getComment() {
            return Optional.ofNullable(comment);
        }
        
        @Override
        public Optional<String> getData() {
            return Optional.ofNullable(data);
        }

        @Override
        public Optional<String> getEvent() {
            return Optional.ofNullable(event);
        }

        @Override
        public Optional<String> getId() {
            return Optional.ofNullable(id);
        }

        @Override
        public Optional<Integer> getRetry() {
            return Optional.ofNullable(retry);
        }
        
        /**
         * @return if event contains data or event info
         */
        boolean isUserEvent() {
            return (getData() != null) || (getEvent() != null);
        }
        
        
        @Override
        public String toString() {
            return toWire();
        }    
        
        
        @Override
        public boolean equals(Object other) {
            return (other instanceof ServerSentEvent) ? ((ServerSentEvent) other).toWire().equals(this.toWire()) : false;
        }
        
        @Override
        public int hashCode() {
           return toWire().hashCode();
        }
    }
}