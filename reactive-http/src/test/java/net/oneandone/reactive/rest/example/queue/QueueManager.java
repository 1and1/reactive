/*
 * QueueManager.java 21.11.2014
 *
 * Copyright (c) 2014 1&1 Internet AG. All rights reserved.
 *
 * $Id$
 */
package net.oneandone.reactive.rest.example.queue;

import java.util.function.Consumer;




public interface QueueManager {
	
	public void publish(String queueName, String message);
	
	public void consume(String queueName, Consumer<String> consumer);
}