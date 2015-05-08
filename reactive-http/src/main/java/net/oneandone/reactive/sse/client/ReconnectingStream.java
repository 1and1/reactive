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


import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import net.oneandone.reactive.sse.ScheduledExceutor;
import net.oneandone.reactive.sse.client.StreamProvider.StreamHandler;
import net.oneandone.reactive.sse.client.StreamProvider.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;




class ReconnectingStream implements Stream {
    private static final Logger LOG = LoggerFactory.getLogger(ReconnectingStream.class);

    private final String id;
    private final AtomicBoolean isOpen = new AtomicBoolean(true);
    
    
    // properties
    private final boolean isFailOnConnectError;
    private final Optional<Duration> connectionTimeout;
    private final URI uri;
    private final String method;
    private final ImmutableMap<String, String> headers;
    private final int numFollowRedirects;
    
    
    // handlers
    private final Consumer<ByteBuffer[]> dataConsumer;
    private final Consumer<Stream> onConnectedListener;
    private final Function<ImmutableMap<String, String>, ImmutableMap<String, String>> headerInterceptor;

   
    // underlying stream
    private final RetryProcessor retryProcessor = new RetryProcessor();
    private final StreamProvider channelProvider;
    private final AtomicReference<StreamProvider.Stream> streamRef;
    
    
    
   
    
    public ReconnectingStream(String id, 
                             URI uri, 
                             String method,
                             ImmutableMap<String, String> headers,
                             boolean isFailOnConnectError,
                             int numFollowRedirects,
                             Optional<Duration> connectionTimeout,
                             Consumer<Stream> onConnectedListener,
                             Consumer<ByteBuffer[]> dataConsumer,
                             Function<ImmutableMap<String, String>, ImmutableMap<String, String>> headerInterceptor) {
        this.id = id;
        this.uri = uri;
        this.method = method;
        this.headers = headers;
        this.numFollowRedirects = numFollowRedirects;
        this.isFailOnConnectError = isFailOnConnectError;
        this.connectionTimeout = connectionTimeout;
        this.onConnectedListener = onConnectedListener;
        this.dataConsumer = dataConsumer;
        this.headerInterceptor = headerInterceptor;
        
        this.channelProvider = NettyBasedChannelProvider.newStreamProvider();
        this. streamRef = new AtomicReference<>(new StreamProvider.NullStream());
    }

    
    public CompletableFuture<Boolean> init() {
        CompletableFuture<Boolean> promise = new CompletableFuture<>();
        
        newStreamAsync().thenAccept(stream -> setUnderlyingStream(stream))
                        .thenAccept(Void -> LOG.debug("[" + id + "] initially connected"))
                        .thenAccept(Void -> promise.complete(true))
                        .exceptionally(error -> { 
                                                    // initial "connect" failed    
                                                    if (isFailOnConnectError) {
                                                        LOG.debug("[" + id + "] initial connect failed", error);
                                                        promise.completeExceptionally(error); 
                    
                                                    // initial "connect" failed, however should be ignored    
                                                    } else {
                                                        LOG.debug("[" + id + "] initial connect failed. Trying to reconnect", error);
                                                        resetUnderlyingStream();
                                                        promise.complete(false);  
                                                    }
                                                    return null;
                                                  });
        
        return promise;
    }

    
 
    public CompletableFuture<Void> writeAsync(String data) {
        return streamRef.get().writeAsync(data);
    }
    
    @Override
    public String getStreamId() {
        return id + "/" + streamRef.get().getStreamId();
    }
    
    @Override
    public boolean isConnected() {
        return streamRef.get().isConnected();
    }
    
    public void terminate() {
        streamRef.getAndSet(new StreamProvider.NullStream()).terminate();
        close();
    }
    
    public void close() {
        closeUnderlyingStream();
        
        if (isOpen.getAndSet(false)) {
            LOG.debug("[" + id + "] close");
            channelProvider.closeAsync();
        }
    } 
   
    
    private void closeUnderlyingStream() {
        if (streamRef.get().isConnected()) {
            LOG.debug("[" + id + "] close underlying stream");
        }
        setUnderlyingStream(new StreamProvider.NullStream());
    }
    
    
    private void setUnderlyingStream(Stream stream) {
        streamRef.getAndSet(stream).close();
        onConnectedListener.accept(stream);
    }        
          
    
    private void resetUnderlyingStream() {
        closeUnderlyingStream();

        if (isOpen.get()) {
            Runnable retryConnect = () -> {
                                            if (!isConnected()) {
                                                newStreamAsync().whenComplete((stream, error) -> { 
                                                                                                    // re"connect" successfully
                                                                                                    if (error == null) {
                                                                                                        if (stream.isConnected()) {
                                                                                                            LOG.debug("[" + id + "] stream reconnected");
                                                                                                        }
                                                                                                        setUnderlyingStream(stream);
                                                                                        
                                                                                                    // re"connect" failed
                                                                                                    } else {
                                                                                                        LOG.debug("[" + id + "] stream reconnected failed");
                                                                                                        resetUnderlyingStream();
                                                                                                    }
                                                                                     });
                                                
                                            }
                                          };
            Duration delay = retryProcessor.scheduleConnect(retryConnect);
            LOG.debug("[" + id + "] schedule reconnect in " + delay.toMillis() + " millis");
        }
    }
    
    
    public boolean isReadSuspended() {
        return streamRef.get().isReadSuspended();
    }

    
    public void resumeRead() {
        streamRef.get().resumeRead();
    }
    
    public void suspendRead() {
        streamRef.get().suspendRead();
    }
    
   
    
    
    @Override
    public String toString() {
        if (!isOpen.get()) {
            return "[closed] " + id;
            
        } else if (streamRef.get().isConnected()) {
            return (streamRef.get().isReadSuspended() ? "[suspended] " : "") + getStreamId();
                
        } else {
            return "[not connected] " + id;
        }
    }
    
    
    
    
    private CompletableFuture<StreamProvider.Stream> newStreamAsync() {
        
        ImmutableMap<String, String> additionalHeaders = headerInterceptor.apply(headers);
        
        if (isOpen.get()) {
            LOG.debug("[" + id + "] open underlying stream (" + method + " " + uri + " - " + Joiner.on("&").withKeyValueSeparator("=").join(additionalHeaders) + ")");
            
            StreamHandler handler = new StreamHandler() {
              
                @Override
                public Optional<StreamHandler> onContent(int channelId, ByteBuffer[] buffers) {
                    dataConsumer.accept(buffers); 
                    return Optional.empty();
                }
                
                @Override
                public void onError(int channelId, Throwable error) {
                    LOG.debug("[" + id + "] - " + channelId + " error occured. reseting underlying stream");
                    resetUnderlyingStream();
                }
            };
            
            
            return channelProvider.openStreamAsync(id, 
                                                   uri,
                                                   method, 
                                                   additionalHeaders,
                                                   isFailOnConnectError,
                                                   numFollowRedirects,
                                                   handler, 
                                                   connectionTimeout);
        } else {
            return CompletableFuture.completedFuture(new StreamProvider.NullStream());
        }
    }
    
    
    

    private static final class RetryProcessor {
        private static final RetrySequence RETRY_SEQUENCE = new RetrySequence(0, 25, 250, 500, 2000, 3000, 4000, 6000);

        private Instant lastSchedule = Instant.now();
        private Duration lastDelay = Duration.ZERO; 
        
        
        public Duration scheduleConnect(Runnable connectTast) {
            
            // last schedule a while ago?
            Duration delaySinceLastSchedule = Duration.between(lastSchedule, Instant.now());
            if (RETRY_SEQUENCE.getMaxDelay().multipliedBy(2).minus(delaySinceLastSchedule).isNegative()) {
                lastDelay = Duration.ZERO;
            } else {
                lastDelay = RETRY_SEQUENCE.nextDelay(lastDelay);
            }
            
            lastSchedule = Instant.now();
            ScheduledExceutor.common().schedule(connectTast, lastDelay.toMillis(), TimeUnit.MILLISECONDS);
            
            return lastDelay;
        }
        

        
        private static final class RetrySequence {
            private final ImmutableMap<Duration, Duration> delayMap;
            private final Duration lastDelay;
            
            public RetrySequence(int... delaysMillis) {
                Map<Duration, Duration> map = Maps.newHashMap();
                for (int i = 0; i < delaysMillis.length; i++) {
                    map.put(Duration.ofMillis(delaysMillis[i]), Duration.ofMillis( (delaysMillis.length > (i+1)) ? delaysMillis[i+1] : delaysMillis[i]) );
                }
                delayMap = ImmutableMap.copyOf(map);
                
                lastDelay = Duration.ofMillis(delaysMillis[delaysMillis.length - 1]);
            }
            
            public Duration nextDelay(Duration previous) {
                return (delayMap.get(previous) == null) ? previous : delayMap.get(previous);
            }
            
            public Duration getMaxDelay() {
                return lastDelay;
            }
        }
    }
}
