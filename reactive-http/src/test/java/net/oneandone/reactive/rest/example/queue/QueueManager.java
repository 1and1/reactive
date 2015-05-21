/*
 * QueueManager.java 21.11.2014
 *
 * Copyright (c) 2014 1&1 Internet AG. All rights reserved.
 *
 * $Id$
 */
package net.oneandone.reactive.rest.example.queue;


import java.io.Closeable;
import java.time.Duration;
import java.util.Optional;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;



public interface QueueManager<T> extends Closeable {
	 
    Subscriber<Message<T>> newSubscriber(String queueName);
	
    Publisher<Message<T>> newPublisher(String queueName);
    
    
	public static class Message<T> {
	    private final String id;
	    private final T data;
	    private final Optional<Duration> ttl;
	    
	    public Message(String id, T data) {
	        this(id, data, null);
        }

	    public Message(String id, T data, Duration ttl) {
	        this.id = id;
	        this.data = data;
	        this.ttl = Optional.ofNullable(ttl);
	    }

	    
        public String getId() {
            return id;
        }

        public T getData() {
            return data;
        }
        
        public Optional<Duration> getTtl() {
            return ttl;
        }

        
        @Override
        public String toString() {
            return "[" + getId() + "] " +  getData();
        }
	}
}