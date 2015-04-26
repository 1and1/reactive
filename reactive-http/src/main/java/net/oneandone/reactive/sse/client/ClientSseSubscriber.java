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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import net.oneandone.reactive.sse.ServerSentEvent;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.io.BaseEncoding;




public class ClientSseSubscriber implements Subscriber<ServerSentEvent> {
    private final AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
    private final StreamProvider.OutboundStream httpUpstream;
    private final StreamProvider streamProvider = NettyBasedStreamProvider.common();

    private final boolean isAutoId;
    private final String globalId = newGlobalId();
    private final AtomicLong nextLocalId = new AtomicLong(1);
    
    
    public ClientSseSubscriber(URI uri) {
        this(uri, true);
    }
    
    
    
    public ClientSseSubscriber(URI uri, boolean isAutoId) {
        this.httpUpstream = new ReconnectingOutboundstream(uri, streamProvider);
        this.isAutoId = isAutoId;
    }
    
    @Override
    public void onSubscribe(Subscription subscription) {
        subscriptionRef.set(subscription);
        subscription.request(1);
    }
    
    @Override
    public void onNext(ServerSentEvent event) {
        
        if ((event.getId() == null) && isAutoId) {
            event = ServerSentEvent.newEvent()
                                   .id(globalId + "-" + nextLocalId.getAndIncrement())
                                   .event(event.getEvent())
                                   .data(event.getData())
                                   .retry(event.getRetry())
                                   .comment(event.getComment());
        }
        
        try {
            httpUpstream.write(event.toWire())
                        .thenAccept(Void -> subscriptionRef.get().request(1));
        } catch (RuntimeException rt) {
            // ...
            subscriptionRef.get().cancel();
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


    @Override
    public void onError(Throwable t) {
        httpUpstream.terminate();
        streamProvider.close();
    }
    
    @Override
    public void onComplete() {
        httpUpstream.close();
        streamProvider.close();
    }
    
    
       
    private static final class ReconnectingOutboundstream implements StreamProvider.OutboundStream  {
        private final URI uri;
        private final StreamProvider streamProvider;
        private StreamProvider.OutboundStream httpUpstream;
        
        public ReconnectingOutboundstream(URI uri, StreamProvider streamProvider) {
            this.uri = uri;
            this.streamProvider = streamProvider;
        }
        
        
        @Override
        public CompletableFuture<Void> write(String msg) {
            try {
                return getHttpstream().write(msg);
            } catch (RuntimeException re) {
                terminateHttp11Upstream();
                throw re;
            }
        }
        
        @Override
        public void terminate() {
            getHttpstream().terminate();
        }
        
        @Override
        public void close() {
            getHttpstream().close();
        }
        
        private synchronized StreamProvider.OutboundStream getHttpstream() {
            if (httpUpstream == null) {
                httpUpstream = streamProvider.newOutboundStream(uri);
            }
            
            return httpUpstream;
        }
        
        
        private synchronized void terminateHttp11Upstream() {
            if (httpUpstream != null) {
                httpUpstream.terminate();;
            }
        }
    }
}