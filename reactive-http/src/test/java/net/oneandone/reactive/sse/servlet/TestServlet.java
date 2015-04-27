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

import java.time.Duration;
import java.util.Map;
import java.util.Queue;
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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;




public class TestServlet extends HttpServlet {
    private static final long serialVersionUID = -7372081048856966492L;

    private final Broker broker = new Broker();
    
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.startAsync().setTimeout(10 * 60 * 1000);

        Publisher<ServerSentEvent> publisher = new ServletSsePublisher(req, resp);
        broker.registerPublisher(req.getPathInfo(), publisher);
    }
    
 
    
    
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.startAsync();
        
        resp.setContentType("text/event-stream");
        Subscriber<ServerSentEvent> subscriber = new ServletSseSubscriber(resp, Duration.ofSeconds(1));
        broker.registerSubscriber(req.getPathInfo(), subscriber);
    }
    
    
    
    
    private static final class Broker {
        
        private final Map<String, ImmutableSet<BrokerSubscription>> subscriptions = Maps.newHashMap();

        public void registerPublisher(String id, Publisher<ServerSentEvent> publisher) {
            publisher.subscribe(new InboundHandler(id));
        }
 
        public void registerSubscriber(String id, Subscriber<ServerSentEvent> subscriber) {
            BrokerSubscription brokerSubscription = new BrokerSubscription(subscriber);
            brokerSubscription.init();
            
            synchronized (subscriptions) {
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
                if (event.getData().equalsIgnoreCase("posion pill")) {
                    subscriptionRef.get().cancel();
                    getSubscriptions(id).forEach(subscription -> subscription.publish(event));
                    getSubscriptions(id).forEach(subscription -> subscription.cancel());
                } else {
                    subscriptionRef.get().request(1);
                    getSubscriptions(id).forEach(subscription -> subscription.publish(event));
                }
            }
        }   
    }

}