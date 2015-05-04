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
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.client.StreamProvider.EmptyOutboundStream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.BaseEncoding;




public class ClientSseSink implements Subscriber<ServerSentEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ClientSseSink.class);
    
    private final AtomicReference<SseOutboundStream> sseOutboundStreamRef = new AtomicReference<>();
    
    private final URI uri;
    private final boolean isAutoId;
    private final Optional<Duration> connectionTimeout;
    private final Optional<Duration> socketTimeout;

    
    public ClientSseSink(URI uri) {
        this(uri, true, Optional.empty(), Optional.empty());
    }

    private ClientSseSink(URI uri, boolean isAutoId, Optional<Duration> connectionTimeout, Optional<Duration> socketTimeout) {
        this.uri = uri;
        this.isAutoId = isAutoId;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
    }
    
    public ClientSseSink autoId(boolean isAutoId) {
        return new ClientSseSink(this.uri, isAutoId, this.connectionTimeout, this.socketTimeout);
    }


    public ClientSseSink connectionTimeout(Duration connectionTimeout) {
        return new ClientSseSink(this.uri, this.isAutoId, Optional.of(connectionTimeout), this.socketTimeout);
    }

    public ClientSseSink socketTimeout(Duration socketTimeout) {
        return new ClientSseSink(this.uri, this.isAutoId, this.connectionTimeout, Optional.of(socketTimeout));
    }


    

    @Override
    public void onSubscribe(Subscription subscription) {
        sseOutboundStreamRef.set(new SseOutboundStream(subscription, uri, isAutoId, connectionTimeout, socketTimeout));
        
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
        private final String id = "cl-out-" + UUID.randomUUID().toString();
        private final boolean isAutoId;
        private final URI uri;
        private final Subscription subscription;
        
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        
        private final String globalId = newGlobalId();
        private final AtomicLong nextLocalId = new AtomicLong(1);
        
        private final StreamProvider streamProvider = NettyBasedStreamProvider.newStreamProvider();
        private final AtomicReference<StreamProvider.OutboundStream> outboundStreamRef = new AtomicReference<>(new StreamProvider.EmptyOutboundStream());

        
        
        public SseOutboundStream(Subscription subscription, 
                                 URI uri, 
                                 boolean isAutoId,
                                 Optional<Duration> connectionTimeout,
                                 Optional<Duration> socketTimeout) {
            this.subscription = subscription;
            this.uri = uri;
            this.isAutoId = isAutoId;
            
            LOG.debug("[" + id + "] opening");
            
            
            streamProvider.newOutboundStream(id, uri, (Void) -> close(), connectionTimeout, socketTimeout)
                          .whenComplete((stream, error) ->  {
                                                              if (error == null) {
                                                                  outboundStreamRef.set(stream); 
                                                                  subscription.request(1);
                                                              } else {
                                                                  terminate(error);
                                                              }
                                                            });
        }
                
        
        public void write(ServerSentEvent event) {
            if (!event.getId().isPresent() && isAutoId) {
                event = ServerSentEvent.newEvent()
                                       .id(globalId + "-" + nextLocalId.getAndIncrement())
                                       .event(event.getEvent().orElse(null))
                                       .data(event.getData().orElse(null))
                                       .retry(event.getRetry().orElse(null))
                                       .comment(event.getComment().orElse(null));
            }
            
            writeInternal(event);
        }
        

        private void writeInternal(ServerSentEvent event) {
            LOG.debug("[" + id + "] writing event " + event.getId().orElse(""));
            outboundStreamRef.get().write(event.toWire())
                                   .whenComplete((Void, error) -> { 
                                                               if (error == null) {
                                                                   subscription.request(1);
                                                               } else {
                                                                   LOG.debug("[" + id + "] error occured by writing event " + event.getId().orElse(""), error);
                                                                   //terminateCurrentHttpStream();
                                                                   subscription.cancel();
                                                               }
                           }); 
        }     
            
       

        
        public void terminate(Throwable t) {
            outboundStreamRef.get().terminate();
        }
        
        
        
        public void close() {
            if (isOpen.getAndSet(false)) {
                LOG.debug("[" + id + "] closing");
                synchronized (this) {
                    outboundStreamRef.getAndSet(new EmptyOutboundStream()).close();
                }
                streamProvider.closeAsync();
                
                subscription.cancel();
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