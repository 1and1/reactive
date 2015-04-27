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


import java.io.ByteArrayOutputStream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import net.oneandone.reactive.sse.ServerSentEvent;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.io.BaseEncoding;




public class ClientSseSubscriber implements Subscriber<ServerSentEvent> {
    
    private final AtomicReference<SseOutboundStream> sseOutboundStreamRef = new AtomicReference<>();
    
    private final URI uri;
    private final boolean isAutoId;
    private final int numRetries;

    
    public ClientSseSubscriber(URI uri) {
        this(uri, true, 0);
    }

    private ClientSseSubscriber(URI uri, boolean isAutoId,int numRetries) {
        this.uri = uri;
        this.isAutoId = isAutoId;
        this.numRetries = numRetries;
    }
    
    public ClientSseSubscriber autoId(boolean isAutoId) {
        return new ClientSseSubscriber(this.uri, isAutoId, this.numRetries);
    }

    
    public ClientSseSubscriber withRetries(int numRetries) {
        return new ClientSseSubscriber(this.uri, this.isAutoId, numRetries);
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        sseOutboundStreamRef.set(new SseOutboundStream(subscription, uri, isAutoId, numRetries));
        
    }
    
    @Override
    public void onNext(ServerSentEvent event) {
        sseOutboundStreamRef.get().write(event);
    }
    
    @Override
    public void onError(Throwable t) {
        sseOutboundStreamRef.get().terminate(t);
    }
    
    @Override
    public void onComplete() {
        sseOutboundStreamRef.get().close();
    }
    
    
    
    private static final class SseOutboundStream {
        private final boolean isAutoId;
        private final int numRetries;
        private final URI uri;
        private final Subscription subscription;
        
        private final String globalId = newGlobalId();
        private final AtomicLong nextLocalId = new AtomicLong(1);
        
        private final StreamProvider streamProvider = NettyBasedStreamProvider.newStreamProvider();
        private StreamProvider.OutboundStream httpUpstream;

        
        
        public SseOutboundStream(Subscription subscription, URI uri, boolean isAutoId, int numRetries) {
            this.subscription = subscription;
            this.uri = uri;
            this.numRetries = numRetries;
            this.isAutoId = isAutoId;
            
            subscription.request(1);
        }
                
        public void write(ServerSentEvent event) {
            if ((event.getId() == null) && isAutoId) {
                event = ServerSentEvent.newEvent()
                                       .id(globalId + "-" + nextLocalId.getAndIncrement())
                                       .event(event.getEvent())
                                       .data(event.getData())
                                       .retry(event.getRetry())
                                       .comment(event.getComment());
            }
            
            write(event.toWire(), numRetries);
        }
        

        private void write(String event, int remainingRetries) {
            getHttpstream().write(event)
                           .whenComplete((Void, error) -> { 
                                                               if (error == null) {
                                                                   subscription.request(1);
                                                               } else {
                                                                   terminateCurrentHttpStream();
                                                                   if (remainingRetries > 0) {
                                                                       write(event, remainingRetries - 1);
                                                                   } else {
                                                                       subscription.cancel();
                                                                   }
                                                               }
                           }); 
        }     
            
       

        
        public void terminate(Throwable t) {
            httpUpstream.terminate();
        }
        
        
        private StreamProvider.OutboundStream getHttpstream() {
            synchronized(this) {
                if (httpUpstream == null) {
                    httpUpstream = streamProvider.newOutboundStream(uri, (Void) -> {   // non-retrying SseOutboundStream? 
                                                                                       if (numRetries == 0) {
                                                                                           close();
                                                                                       }; 
                                                                                   });
                }
                
                return httpUpstream;
            }
        }
        
        
        public void close() {
            synchronized (this) {
                if (httpUpstream != null) {
                    httpUpstream.close();
                    httpUpstream = null;
                }
            }
            streamProvider.close();
            
            subscription.cancel();
        }
        
        private synchronized void terminateCurrentHttpStream() {
            synchronized (this) {
                if (httpUpstream != null) {
                    httpUpstream.terminate();
                    httpUpstream = null;
                }
            }
        }
        
        
        private static String newGlobalId() {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                
                UUID uuid = UUID.randomUUID();
                dos.writeLong(uuid.getMostSignificantBits());
                dos.writeLong(uuid.getLeastSignificantBits());
                dos.flush();
                
                return BaseEncoding.base64Url().omitPadding().encode(baos.toByteArray());
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
    }
}