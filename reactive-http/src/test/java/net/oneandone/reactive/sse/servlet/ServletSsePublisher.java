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
package net.oneandone.reactive.sse.servlet;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.utils.SubscriberNotifier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Maps the servlet input stream (which have to receive Server-sent events) into a publisher 
 */
@Deprecated
public class ServletSsePublisher implements Publisher<ServerSentEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ServletSsePublisher.class);
    
    private final HttpServletResponse resp;
    
    private boolean subscribed = false; // true after first subscribe
    private final ServletInputStream inputStream;
    private final Consumer<Void> closeListener;
    private final Consumer<Throwable> errorListener;

    

    public ServletSsePublisher(HttpServletRequest req, HttpServletResponse resp) {
        LOG.debug(req.getMethod() + " " + req.getRequestURL().toString());
        
        try {
            this.resp = resp;
            this.inputStream = req.getInputStream();
            this.closeListener = (Void) -> sendNoContentResponse(resp);
            this.errorListener = (error) -> sendServerError(resp, error);
            
            if (!req.isAsyncStarted()) {
                req.startAsync().setTimeout(Long.MAX_VALUE);  // tomcat 7 default is 30 sec 
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    
    
    private void startReceiving() {
        try {
            resp.flushBuffer();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    
    
    @Override
    public void subscribe(Subscriber<? super ServerSentEvent> subscriber) {
        synchronized (this) {
            // https://github.com/reactive-streams/reactive-streams-jvm#1.9
            if (subscriber == null) {  
                throw new NullPointerException("subscriber is null");
            }
            
            if (subscribed == true) {
                subscriber.onError(new IllegalStateException("subscription already exists. Multi-subscribe is not supported"));  // only one allowed
            } else {
                startReceiving();
                
                subscribed = true;
                SEEEventReaderSubscription subscription = new SEEEventReaderSubscription(inputStream, subscriber, closeListener, errorListener);
                subscription.init();
            }
        }
    }

    
    
    private static void sendNoContentResponse(HttpServletResponse resp) {
        try {
            resp.setStatus(204);
            resp.getWriter().close();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    
    
    private static void sendServerError(HttpServletResponse resp, Throwable error) {
        try {
            resp.setStatus(500);
            resp.setHeader("Connection", "close");
            resp.getWriter().close();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    
    
    private static class SEEEventReaderSubscription implements Subscription {
        private final SubscriberNotifier<ServerSentEvent> subscriberNotifier;
        private final SseInboundChannel channel;
        private final Consumer<Void> closeListener;
        private final Consumer<Throwable> errorListener;
        private final AtomicBoolean isOpen = new AtomicBoolean(true);

        public SEEEventReaderSubscription(ServletInputStream inputStream, Subscriber<? super ServerSentEvent> subscriber, 
                                          Consumer<Void> closeListener,
                                          Consumer<Throwable> errorListener) {
            this.subscriberNotifier = new SubscriberNotifier<>(subscriber, this);
            this.closeListener = closeListener;
            this.errorListener = errorListener;
            
            this.channel = new SseInboundChannel(inputStream,                                      // servlet input stream
                                                 event -> subscriberNotifier.notifyOnNext(event),  // event consumer
                                                 error -> notifyError(error),                      // error consumer
                                                 Void -> close());                                 // completion consumer         
        }
        
        void init() {
            subscriberNotifier.start();
        }

        @Override
        public void cancel() {
            close();
        }
        
        
        private void close() {
            if (isOpen.getAndSet(false)) {
                closeListener.accept(null);
                subscriberNotifier.notifyOnComplete();
                
                channel.close();
            }
        }
        
        private void notifyError(Throwable error) {
            errorListener.accept(error); 
            subscriberNotifier.notifyOnError(error); 
            
            close();
        }
        
        
        
        @Override
        public void request(long n) {
            if(n <= 0) {
                // https://github.com/reactive-streams/reactive-streams#3.9
                subscriberNotifier.notifyOnError(new IllegalArgumentException("Non-negative number of elements must be requested: https://github.com/reactive-streams/reactive-streams#3.9"));
            } else {
                channel.request(n);
            }
        }
        
        
        @Override
        public String toString() {
            return channel.toString();
        }
    }
}        
    

