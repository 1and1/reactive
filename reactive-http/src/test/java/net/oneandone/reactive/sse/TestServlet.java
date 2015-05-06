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




import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.servlet.ServletSsePublisher;
import net.oneandone.reactive.sse.servlet.ServletSseSubscriber;
import net.oneandone.reactive.utils.SubscriberNotifier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;




public class TestServlet extends HttpServlet {
    private static final long serialVersionUID = -7372081048856966492L;

    private final Broker broker = new Broker();
    
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.startAsync().setTimeout(10 * 60 * 1000);
        
        resp.setStatus(200);
        resp.flushBuffer();

        Publisher<ServerSentEvent> publisher = new ServletSsePublisher(req, resp);
        broker.registerPublisher(normalizeId(req.getPathInfo()), publisher);
    }
    
 
    
    
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        
        if (req.getPathInfo().startsWith("/brokerinfo")) {
            resp.setContentType("text/plain");
            resp.getWriter().write(broker.toString());
            
        } else if (req.getPathInfo().startsWith("/redirect")) {
            System.out.println(req.getQueryString());
            int num = Integer.parseInt(req.getParameter("num"));
            if (num > 0)  {
                resp.sendRedirect(req.getRequestURL().toString() + "?num=" + (num  - 1));
            } else {
                resp.sendRedirect(req.getRequestURL().toString().replace("redirect", "channel"));
            }

        } else if (req.getPathInfo().startsWith("/notfound")) {
            resp.sendError(404);

        } else if (req.getPathInfo().startsWith("/servererror")) {
            resp.sendError(500);

        } else {
            req.startAsync();
            
            String lastEventId = req.getHeader("Last-Event-ID");
            resp.setContentType("text/event-stream");
            Subscriber<ServerSentEvent> subscriber = new ServletSseSubscriber(resp, Duration.ofSeconds(1));
            broker.registerSubscriber(normalizeId(req.getPathInfo()), subscriber, lastEventId);
        }
    }
    
    
    private String normalizeId(String id) {
        id = id.trim();
        if (id.startsWith("/")) {
            id = id.substring(1, id.length());
        }
        if (id.endsWith("/")) {
            id = id.substring(0, id.length() - 1);
        }
        
        return id;
    }
    
    
    private static final class Broker {
        
        private final Map<String, ImmutableSet<BrokerSubscription>> subscriptions = Maps.newHashMap();
        private final Map<String, Instant> deepSleepTime = Maps.newHashMap();

        private final Map<String, ServerSentEvent> eventHistoryCache = Collections.synchronizedMap(new LinkedHashMap<String, ServerSentEvent>() {
            private static final long serialVersionUID = -1640442197943481724L;

            protected boolean removeEldestEntry(Map.Entry<String,ServerSentEvent> eldest) {
                return (size() > 100);
            }
        });
        

        public synchronized void registerPublisher(String id, Publisher<ServerSentEvent> publisher) {
            if (isKnockedOut(id)) {
                throw new RuntimeException("knockout drops");
            }
            publisher.subscribe(new InboundHandler(id));
        }
 
        public synchronized void registerSubscriber(String id, Subscriber<ServerSentEvent> subscriber, String lastEventId) {
            if (isKnockedOut(id)) {
                throw new RuntimeException("knockout drops");
            }
            
            BrokerSubscription brokerSubscription = new BrokerSubscription(subscriber);
            brokerSubscription.init();
            
            synchronized (subscriptions) {
                if (lastEventId != null) {
                    boolean isReplaying = false;
                    for (Entry<String, ServerSentEvent> entry : eventHistoryCache.entrySet()) {
                        if (isReplaying) {
                            brokerSubscription.publish(entry.getValue());
                        }
                        
                        if (entry.getKey().equals(lastEventId)) {
                            isReplaying = true;
                        }
                    }
                }
                
                
                ImmutableSet<BrokerSubscription> subs = getSubscriptions(id);
                subscriptions.put(id, ImmutableSet.<BrokerSubscription>builder().addAll(subs).add(brokerSubscription).build());
            }
        }
        
        
        private  ImmutableSet<BrokerSubscription> getSubscriptions(String id) {
            synchronized (subscriptions) {
                ImmutableSet<BrokerSubscription> subs = subscriptions.get(id);
                if (subs == null) {
                    return ImmutableSet.of();
                } else {
                    return subs;
                }
            }
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            subscriptions.forEach((id, subscripotions) -> sb.append(id + " -> " + Joiner.on(", ").join(subscripotions)));
            
            return sb.toString();
        }
        
        
        private final boolean isKnockedOut(String id) {
            Instant toTime = deepSleepTime.get(id);
            if (toTime != null) {
                boolean isKnockedOut = Instant.now().isBefore(toTime);
                if (!isKnockedOut) {
                    deepSleepTime.remove(id);
                }
                return isKnockedOut;
            }
            return false;
        }

        
        private static final class BrokerSubscription implements Subscription {
            
            private final SubscriberNotifier<ServerSentEvent> subscriberNotifier;
            
            private final AtomicLong pendingRequests = new AtomicLong();
            private final Queue<ServerSentEvent> sendQueue = Queues.newConcurrentLinkedQueue();


            public BrokerSubscription(Subscriber<? super ServerSentEvent> subscriber) {
                this.subscriberNotifier = new SubscriberNotifier<>(subscriber, this);
            }
            
            void init() {
                subscriberNotifier.start();
            }
            
            
            @Override
            public void request(long n) {
                pendingRequests.addAndGet(n);
                process();
            }
            
            
            @Override
            public void cancel() {
                subscriberNotifier.notifyOnComplete();
            }
            

            public void publish(ServerSentEvent event) {
                sendQueue.add(event);
                process();
            }
            
            private void process() {
                synchronized (pendingRequests) {
                    while (pendingRequests.get() > 0) {
                        if (sendQueue.isEmpty()) {
                            return;
                        } else {
                            ServerSentEvent event = sendQueue.poll();
                            pendingRequests.decrementAndGet();
                            subscriberNotifier.notifyOnNext(event);
                        }
                    }                
                }
            }
        }
        
        
        private final class InboundHandler implements Subscriber<ServerSentEvent> {

            private final String id; 
            private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
          
            
            public InboundHandler(String id) {
                this.id = id;
            }
            
            @Override
            public void onSubscribe(Subscription subscription) {
                subscriptionRef.set(subscription);
                subscription.request(1);
            }

            @Override
            public void onComplete() {
                subscriptionRef.get().cancel();
                getSubscriptions(id).forEach(subscription -> subscription.cancel());
            }
            
            @Override
            public void onError(Throwable t) {
                subscriptionRef.get().cancel();
                getSubscriptions(id).forEach(subscription -> subscription.cancel());
            }
            
            @Override
            public void onNext(ServerSentEvent event) {
                
                if (event.getEvent().orElse("").equalsIgnoreCase("posion pill")) {
                    subscriptionRef.get().cancel();
                    getSubscriptions(id).forEach(subscription -> subscription.cancel());
                
                } else if (event.getEvent().orElse("").equalsIgnoreCase("knockout drops")) {
                    String millis = event.getData().orElse("100");
                    deepSleepTime.put(id, Instant.now().plusMillis(Long.parseLong(millis)));
                    subscriptionRef.get().cancel();
                    getSubscriptions(id).forEach(subscription -> subscription.cancel());
                    
                    
                } else {
                    publish(event);
                    String eventId = event.getId().orElse(UUID.randomUUID().toString());
                    eventHistoryCache.put(eventId, event);
                    subscriptionRef.get().request(1);
                }
            }
            
            
            private void publish(ServerSentEvent event) {
                getSubscriptions(id).forEach(subscription ->  subscription.publish(event));
            }
        }   
    }

}