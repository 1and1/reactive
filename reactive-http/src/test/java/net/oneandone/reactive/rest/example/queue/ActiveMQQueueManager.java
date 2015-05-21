/*
 * ProcessingQueueActiveMQImpl.java 21.11.2014
 *
 * Copyright (c) 2014 1&1 Internet AG. All rights reserved.
 *
 * $Id$
 */
package net.oneandone.reactive.rest.example.queue;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;

import com.google.common.collect.Lists;




public class ActiveMQQueueManager extends AbstractQueueManager<String> implements Closeable {
    private final BrokerService broker;
    private final Connection connection;
    private final List<QueueImpl> openQueues = Lists.newArrayList();
	
	
	public ActiveMQQueueManager(File dataDirectory) {
	    try  {
    		broker = new BrokerService();
    		broker.addConnector("tcp://localhost:0");
    		broker.setPersistent(true);
    		broker.setDataDirectoryFile(dataDirectory);
    		broker.setSchedulerSupport(true);
    		broker.start();
    			
    		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri());
    		connection = connectionFactory.createConnection();
    		connection.start();
	    } catch (Exception e) {
	        throw new RuntimeException(e);
	    }
	}
	
	@Override
	public void close() throws IOException {
	    try {
	        synchronized (openQueues) {
	            openQueues.forEach(queue -> queue.close());
	            openQueues.clear();
            }
	        
	        connection.close();
	        broker.stop();
	    } catch (Exception e) {
	        throw new IOException(e);
	    }
	}
	

	@Override
	protected MessageSink<String> newConsumer(String queueName) {
	    return new QueueImpl(connection, queueName, null);
	}
    

	@Override
	protected MessageSource<String> newMessageSource(String queueName, Consumer<Message<String>> consumer) {
        return new QueueImpl(connection, queueName, consumer);
    }
	
	

    private static final class QueueImpl implements MessageSink<String>, MessageSource<String> {
        private final MessageProducer producer;
        private final Queue destination; 
        private final Session session;
        
        
        public QueueImpl(Connection connection, String name, Consumer<Message<String>> consumer) {
            try {
                this.session = connection.createSession(true, Session.SESSION_TRANSACTED);         
                this.destination = session.createQueue(name);
                this.producer = session.createProducer(destination);
                this.producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                
                if (consumer != null) {
System.out.println("register listener");
                    session.createConsumer(destination).setMessageListener(new MessageConsumer(consumer));
                }
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }
        

        @Override
        public void close() {
            try {
                session.close();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }


        
        @Override
        public void accept(Message<String> message) {
            try {
                TextMessage msg = session.createTextMessage(message.getData());
                msg.setStringProperty("id",message.getId());
                
                if (message.getTtl().isPresent()) {
                    producer.send(msg, javax.jms.Message.DEFAULT_DELIVERY_MODE, javax.jms.Message.DEFAULT_PRIORITY, message.getTtl().get().toMillis());
                } else {
                    producer.send(msg);
                }
                session.commit();
System.out.println("send jms " + message);                
                
            } catch (JMSException | RuntimeException e) {
                throw new RuntimeException(e);
            }
        }   
        
        
        @Override
        public void suspend() {
            
        }
        
        @Override
        public void resume() {
            
        }
        
        
        private final class MessageConsumer implements MessageListener {
            private final Consumer<Message<String>> consumer;
            
            public MessageConsumer(Consumer<Message<String>> consumer) {
                this.consumer = consumer;
            }
            
            @Override
            public void onMessage(javax.jms.Message msg) {
                try {
                    Message<String> message = new Message<String>(msg.getStringProperty("id"), ((TextMessage) msg).getText());
                    consumer.accept(message);
                    session.commit();

System.out.println("received jms " + message);                    
                } catch (JMSException e) {
                    close();
                }
            }
        }
    }
}
