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
package net.oneandone.reactive.sse.client;


import java.io.Closeable;
import java.net.URI;

import net.oneandone.reactive.sse.ServerSentEvent;

import org.reactivestreams.Subscriber;



public class SseClient implements Closeable {
    private final StreamProvider streamProvider = new NettyBasedStreamProvider();
    private final URI uri;
    private final boolean isAutoId;
    
    private SseClient(URI uri, boolean isAutoId) {
        this.uri = uri;
        this.isAutoId = isAutoId;
    }
    
    
    public static SseClient target(URI uri) {
        return new SseClient(uri, true);
    }
    
    public SseClient autoId(boolean isAutoId) {
        return new SseClient(this.uri, isAutoId);
    }
    
    public SseOutboundStream outbound() {
        
        return new SseOutboundStream() {
            @Override
            public Subscriber<ServerSentEvent> subscriber() {
                return new SseOutboundSubscriber(uri, streamProvider, isAutoId);
            }
        };
    }
    
    
    public SseInboundStream inbound() {
        return new SseInboundStreamImpl();
    }
    
    
    private final class SseInboundStreamImpl implements SseInboundStream {
        private boolean subscribed = false; // true after first subscribe
        
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
                    subscribed = true;
                    new SseInboundStreamSubscription(uri, streamProvider, subscriber); // will call subscriber#onSubscribe
                }
            }   
        }
    }
    

    @Override
    public void close() {
        streamProvider.close();
    }
}  