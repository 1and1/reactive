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
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;




public class TestServlet extends HttpServlet {
    private static final Logger LOG = LoggerFactory.getLogger(TestServlet.class);
    
    private static final long serialVersionUID = -7372081048856966492L;

    private final Broker broker = new Broker();

    
    @Override
    public void destroy() {
        broker.close();
    }
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        
        if (req.getPathInfo().startsWith("/redirect")) {
            resp.setStatus(307);
            int num = Integer.parseInt(req.getParameter("num"));
            if (num > 0)  {
                resp.setHeader("location", req.getRequestURL().toString() + "?num=" + (num  - 1));
            } else {
                resp.setHeader("location", req.getRequestURL().toString().replace("redirect", "channel"));
            }

        } else if (req.getPathInfo().startsWith("/notfound")) {
            resp.sendError(404);

        } else if (req.getPathInfo().startsWith("/servererror")) {
            resp.sendError(500);

        } else {
            Publisher<ServerSentEvent> publisher = new ServletSsePublisher(req, resp);
            broker.registerPublisher(normalizeId(req.getPathInfo()), publisher);
        }
    }
    
 
    
    
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        
        if (req.getPathInfo().startsWith("/brokerinfo")) {
            resp.setContentType("text/plain");
            resp.getWriter().write(broker.toString());
            
        } else if (req.getPathInfo().startsWith("/redirect")) {
            resp.setStatus(303);
            int num = Integer.parseInt(req.getParameter("num"));
            if (num > 0)  {
                resp.setHeader("location", req.getRequestURL().toString() + "?num=" + (num  - 1));
            } else {
                resp.setHeader("location", req.getRequestURL().toString().replace("redirect", "channel"));
            }

        } else if (req.getPathInfo().startsWith("/notfound")) {
            resp.sendError(404);

        } else if (req.getPathInfo().startsWith("/servererror")) {
            resp.sendError(500);

        } else {
            String lastEventId = req.getHeader("Last-Event-ID");
            resp.setContentType("text/event-stream");
            
            String keepAlivePeriod = req.getParameter("keepaliveperiod");
            if (keepAlivePeriod == null) {
                keepAlivePeriod = "60";
            }
            Subscriber<ServerSentEvent> subscriber = new ServletSseSubscriber(req, resp, Duration.ofSeconds(Integer.parseInt(keepAlivePeriod)));
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

        private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(0);
        private final Map<String, Map<String, String>> eventHistoryCache = Maps.newHashMap();
        
        
        public synchronized void registerPublisher(String channelId, Publisher<ServerSentEvent> publisher) {
            if (isKnockedOut(channelId)) {
                throw new RuntimeException("knockout drops");
            }
            publisher.subscribe(new InboundHandler(channelId));
        }
 
        public synchronized void registerSubscriber(String channelId, Subscriber<ServerSentEvent> subscriber, String lastEventId) {
            if (isKnockedOut(channelId)) {
                throw new RuntimeException("knockout drops");
            }
            
            BrokerSubscription brokerSubscription = new BrokerSubscription(subscriber);
            brokerSubscription.init();
            
            synchronized (subscriptions) {
                if (lastEventId != null) {
                    
                    synchronized (eventHistoryCache) {
                        Map<String, String> events = eventHistoryCache.get(channelId);

                        if (events != null) {
                            boolean isReplaying = false;
                            for (Entry<String, String> entry : events.entrySet()) {
                                if (isReplaying) {
                                    brokerSubscription.publish(ServerSentEvent.newEvent().id(entry.getKey()).data(entry.getValue()));
                                }
                                
                                if (entry.getKey().equals(lastEventId)) {
                                    isReplaying = true;
                                }
                            }
                        }
                    }
                }
                
                
                ImmutableSet<BrokerSubscription> subs = getSubscriptions(channelId);
                subscriptions.put(channelId, ImmutableSet.<BrokerSubscription>builder().addAll(subs).add(brokerSubscription).build());
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
        
        
        public void close() {
            executor.shutdown();
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

            private final String channelId; 
            private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
            private final AtomicReference<FlowControl> flowControlRef = new AtomicReference<>(new Awake());
            
            
            public InboundHandler(String channelId) {
                this.channelId = channelId;
            }
            
            @Override
            public void onSubscribe(Subscription subscription) {
                subscriptionRef.set(subscription);
                subscription.request(1);
            }

            @Override
            public void onComplete() {
                LOG.debug("completed");
                subscriptionRef.get().cancel();
                getSubscriptions(channelId).forEach(subscription -> subscription.cancel());
            }
            
            @Override
            public void onError(Throwable t) {
                LOG.debug("error " + t);
                subscriptionRef.get().cancel();
                getSubscriptions(channelId).forEach(subscription -> subscription.cancel());
            }
            
            @Override
            public void onNext(ServerSentEvent event) {
                LOG.debug("received\r\n" + event);
                
                if (event.getEvent().orElse("").equalsIgnoreCase("posion pill")) {
                    subscriptionRef.get().cancel();
                    getSubscriptions(channelId).forEach(subscription -> subscription.cancel());
                
                } else if (event.getEvent().orElse("").equalsIgnoreCase("knockout drops")) {
                    String millis = event.getData().orElse("100");
                    deepSleepTime.put(channelId, Instant.now().plusMillis(Long.parseLong(millis)));
                    subscriptionRef.get().cancel();
                    getSubscriptions(channelId).forEach(subscription -> subscription.cancel());
                    
                } else if (event.getEvent().orElse("").equalsIgnoreCase("soporific")) {
                    String millis = event.getData().orElse("100");
                    flowControlRef.set(new Napping(Duration.ofMillis(Long.parseLong(millis))));
                    
                } else {
                    publish(event);
                    
                    synchronized (eventHistoryCache) {
                        Map<String, String> events = eventHistoryCache.get(channelId);
                        if (events == null) {
                            events = new LinkedHashMap<String, String>() {
                                                    private static final long serialVersionUID = -1640442197943481724L;
    
                                                    protected boolean removeEldestEntry(Map.Entry<String,String> eldest) {
                                                        return (size() > 100);
                                                    }
                                    }; 
                                    
                           eventHistoryCache.put(channelId, events);
                        }
                        events.put(event.getId().orElse(UUID.randomUUID().toString()), event.getData().orElse(""));
                    }
                }
                
                flowControlRef.get().onEvent();
            }
            
            
            private void publish(ServerSentEvent event) {
                getSubscriptions(channelId).forEach(subscription ->  subscription.publish(event));
            }
            
            
            private final class Awake implements FlowControl {

                @Override
                public void onEvent() {
                    subscriptionRef.get().request(1);
                }
            }
        
            private final class Napping implements  FlowControl {
                private final AtomicLong numPendingRequests = new AtomicLong();
                
                public Napping(Duration delay) {
                    executor.schedule(() -> wakeUp(), delay.toMillis(), TimeUnit.MILLISECONDS);
                }
                
                public synchronized void onEvent() {
                    numPendingRequests.incrementAndGet();
                }

                
                public synchronized void wakeUp() {
                    subscriptionRef.get().request(numPendingRequests.get());
                    numPendingRequests.set(0);
                    
                    flowControlRef.set(new Awake());
                }
            }
        } 
        
        
        private static interface FlowControl {
            
            void onEvent();
        }
    }
}