/*
 * Cbopyright 1&1 Internet AG, https://github.com/1and1/
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
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.utils.IllegalStateSubscription;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




/**
 * Maps Server-sent events (reactive) stream into a servlet output streamr 
 * 
 */
public class ServletSseSubscriber implements Subscriber<ServerSentEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ServletSseSubscriber.class);

    
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>(new IllegalStateSubscription());
    
    private final SseOutboundChannel channel;

    
    
    public ServletSseSubscriber(HttpServletRequest req, HttpServletResponse resp) {
        this(req, resp, Duration.ofSeconds(25));
    }   

    
    public ServletSseSubscriber(HttpServletRequest req, HttpServletResponse resp, Duration keepAlivePeriod) {
        if (!req.isAsyncStarted()) {
            req.startAsync().setTimeout(Long.MAX_VALUE);  // tomcat 7 default is 30 sec 
        }
        
        try {
            this.channel = new SseOutboundChannel(resp.getOutputStream(), error -> onError(error), keepAlivePeriod);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }   



   
   
    
    @Override
    public void onSubscribe(Subscription subscription) {
        subscriptionRef.set(subscription);
        subscriptionRef.get().request(25);  // request next 25 
    }

    
    
    @Override
    public void onNext(ServerSentEvent event) {
        channel.writeEventAsync(event)           
               .thenAccept(written -> subscriptionRef.get().request(1)); 
    }

    @Override
    public void onError(Throwable t) {
        LOG.debug("error on source stream. stop streaming " + t.getMessage());
        close();
    }
  
    @Override
    public void onComplete() {
        close();
    }
    

    private void close() {
        if (isOpen.getAndSet(false)) {
            subscriptionRef.get().cancel();
            channel.close();          
        }
    }
}
