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
import java.nio.channels.ClosedChannelException;
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
import net.oneandone.reactive.sse.client.HttpChannelProvider.ConnectionParams;
import net.oneandone.reactive.sse.client.HttpChannelDataHandler;
import net.oneandone.reactive.sse.client.HttpChannel;
import net.oneandone.reactive.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;




class ReconnectingHttpChannel implements HttpChannel {
    private static final Logger LOG = LoggerFactory.getLogger(ReconnectingHttpChannel.class);

    
    private final String id;
    private final AtomicBoolean isOpen = new AtomicBoolean(true);

    
    // properties
    private final boolean isFailOnConnectError;
    private final Optional<Duration> connectTimeout;
    private final URI uri;
    private final String method;
    private final ImmutableMap<String, String> headers;
    private final int numFollowRedirects;
    
    
    // handlers
    private final HttpChannelDataHandler dataHandler;
    private final Consumer<Boolean> isWriteableStateChangedListener;
    private final Function<ImmutableMap<String, String>, ImmutableMap<String, String>> headerInterceptor;

   
    // underlying channel
    private final AtomicReference<HttpChannel> channelRef = new AtomicReference<>(new HttpChannel.NullHttpChannel(false));
    private final HttpChannelProvider channelProvider = HttpChannelProviderFactory.newHttpChannelProvider();
    private final RetryScheduler retryProcessor = new RetryScheduler();
    private final Object reconnectingLock = new Object();
    private boolean isReconnecting = false;

    
   
    
    public ReconnectingHttpChannel(String id, 
                                   URI uri, 
                                   String method,
                                   ImmutableMap<String, String> headers,
                                   boolean isFailOnConnectError,
                                   int numFollowRedirects,
                                   Optional<Duration> connectTimeout,
                                   Consumer<Boolean> isWriteableStateChangedListener,
                                   HttpChannelDataHandler dataHandler,
                                   Function<ImmutableMap<String, String>, ImmutableMap<String, String>> headerInterceptor) {
        this.id = id;
        this.uri = uri;
        this.method = method;
        this.headers = headers;
        this.numFollowRedirects = numFollowRedirects;
        this.isFailOnConnectError = isFailOnConnectError;
        this.connectTimeout = connectTimeout;
        this.isWriteableStateChangedListener = isWriteableStateChangedListener;
        this.dataHandler = dataHandler;
        this.headerInterceptor = headerInterceptor;
    }

    
    @Override
    public String getId() {
        return getCurrentHttpChannel().getId();
    }
    
    
    @Override
    public boolean isOpen() {
        return getCurrentHttpChannel().isOpen();
    }
    
    
    public CompletableFuture<Boolean> init() {
        return newHttpChannelAsync().thenApply(channel -> { 
                                                            LOG.debug("[" + id + "] initially connected");
                                                            replaceCurrentChannel(channel); 
                                                            return true;
                                                          })
                                                
                                    .exceptionally(error -> { 
                                                            // initial "connect" failed
                                                            if (isFailOnConnectError) {
                                                                LOG.debug("[" + id + "] initial connect failed. " + error.getMessage());
                                                                throw Utils.propagate(error);
                                                 
                                                            // initial "connect" failed, however should be ignored
                                                            } else { 
                                                                LOG.debug("[" + id + "] initial connect failed. " + error.getMessage() + " Trying to reconnect");
                                                                initiateReconnect(error);
                                                                return false;
                                                            }
                                                           });
    }

    
    @Override
    public CompletableFuture<Void> writeAsync(String data) {
        return getCurrentHttpChannel().writeAsync(data)
                                      .exceptionally(error -> { 
                                                                  initiateReconnect(error);   
                                                                  throw Utils.propagate(error); 
                                                              });
    }
    
  
    @Override
    public boolean isReadSuspended() {
        return getCurrentHttpChannel().isReadSuspended();
    }

    @Override
    public void suspendRead(boolean isSuspended) {
        // [why sync?] replacing ref (see below) includes transferring suspended state 
        synchronized (channelRef) {
            getCurrentHttpChannel().suspendRead(isSuspended);
        }
    }
    
    
    @Override
    public String toString() {
        if (!isOpen.get()) {
            return "[closed] " + id;
            
        } else if (getCurrentHttpChannel().isOpen()) {
            return (getCurrentHttpChannel().isReadSuspended() ? "[suspended] " : "") + getId();
                
        } else {
            return "[not connected] " + id;
        }
    }
    
    
    private HttpChannel getCurrentHttpChannel() {
        return channelRef.get();
    }
    
  

    
    private CompletableFuture<HttpChannel> newHttpChannelAsync() { 
       
        if (isOpen.get()) {
            // process headers interceptor
            ImmutableMap<String, String> additionalHeaders = headerInterceptor.apply(headers);
            
            LOG.debug("[" + id + "] open underlying channel (" + method + " " + uri + " - " + Joiner.on("&").withKeyValueSeparator("=").join(additionalHeaders) + ")");
            
            HttpChannelDataHandler handler = new HttpChannelDataHandler() {
              
                @Override
                public void onContent(String id, ByteBuffer[] data) {
                    dataHandler.onContent(id, data); 
                }
                
                @Override
                public void onError(String id, Throwable error) {
                    if (isOpen.get()) {
                        LOG.debug("[" + id + "]  error " + error.toString() + " occured. Trying to reconnect");                       
                        initiateReconnect(error);  
                    }
                }
            };
            
            
            return channelProvider.newHttpChannelAsync(new ConnectionParams(id, 
                                                                            uri, 
                                                                            method, 
                                                                            additionalHeaders, 
                                                                            numFollowRedirects, 
                                                                            handler, 
                                                                            connectTimeout));
            
            
        } else {
            return CompletableFuture.completedFuture(new HttpChannel.NullHttpChannel(true));
        }
    }
    
  
    
    @Override
    public void terminate() {
        synchronized (reconnectingLock) {
            teminateCurrentChannel(new ClosedChannelException());
        }

        close();
    }
    
    
    @Override
    public void close() {
        if (isOpen.getAndSet(false)) {
            closeCurrentChannel(new ClosedChannelException());
            
            channelProvider.closeAsync();
            LOG.debug("[" + id + "] closed");
        }
    } 
   
    
    
    
    ///////////////////////////////////////////
    // (re)connect handling 
    
    
    private void teminateCurrentChannel(Throwable error) {
        channelRef.get().terminate();  // terminate -> end chunk should NOT be written (refer chunked-transfer encoding)
        closeCurrentChannel(error);
    }        
    
    
    private void closeCurrentChannel(Throwable error) {
        // replace current one with null channel (closes old implicitly)
        replaceCurrentChannel(new HttpChannel.NullHttpChannel(true));          
        
        // notify close of former channel (-> reset sse parser)
        dataHandler.onError(id, (error == null) ? new ClosedChannelException() : error);
    }        
    
    
    private void replaceCurrentChannel(HttpChannel channel) {
        // [why sync?] replacing ref includes transferring suspended state 
        synchronized (channelRef) {
            // close current channel
            channelRef.get().close();
            
            // restore suspend state for new channel
            channel.suspendRead(channelRef.get().isReadSuspended());
            
            // set new channel
            channelRef.set(channel);
            isWriteableStateChangedListener.accept(true);
        }
    }        
    
    
    
    private void initiateReconnect(Throwable t) {
        // This method will be called, if 
        // * channel read operation fails
        // * channel write operation fails
        // * reconnect fails
        

        // first, close current channel 
        closeCurrentChannel(t);


        
        // [sync] parallel reconnection task have to be avoided
        // for this reason, first it will be checked if a reconnecting
        // task is already running 
        synchronized (reconnectingLock) {
            
            if (isOpen.get()) {
                
                if (!isReconnecting) {
                    isReconnecting = true;
    
                    Runnable retryConnect = () -> {
                                                    newHttpChannelAsync().whenComplete((channel, error) -> {
                                                        
                                                        // [sync] ensure that the exit state 
                                                        // of this reconnecting task is either 
                                                        // * a valid connection, 
                                                        // * a newly initiated reconnect or
                                                        // * an unchanged state (if the reconnecting channel is already closed meanwhile)
                                                        synchronized (reconnectingLock) {
                                                            isReconnecting = false;
                                                            
                                                            if (isOpen.get()) {
                                                                
                                                                if (error == null) {
                                                                    if (channel.isOpen()) {
                                                                        LOG.debug("[" + id + "] channel reconnected");
                                                                    }
                                                                    replaceCurrentChannel(channel);
                                                                    
                                                                } else {
                                                                    if (isOpen.get()) {
                                                                        LOG.debug("[" + id + "] channel reconnect failed. Trying again");
                                                                        initiateReconnect(error);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                     });
                     };
                     
                     Duration delay = retryProcessor.scheduleWithDelay(retryConnect);
                     LOG.debug("[" + id + "] schedule reconnect in " + delay.toMillis() + " millis");
                }
            }
        }
    }
    
    
    
    

    private static final class RetryScheduler {
        private static final RetrySequence RETRY_SEQUENCE = new RetrySequence(0, 3, 25, 250, 500, 2000, 3000, 4000, 6000);

        private Instant lastSchedule = Instant.now();
        private Duration lastDelay = Duration.ZERO; 
        
        
        public Duration scheduleWithDelay(Runnable connectTast) {
            
            // last schedule a while ago?
            Duration delaySinceLastSchedule = Duration.between(lastSchedule, Instant.now());
            if (RETRY_SEQUENCE.getMaxDelay().multipliedBy(2).minus(delaySinceLastSchedule).isNegative()) {
                // yes
                lastDelay = Duration.ZERO;
            } else {
                // no
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
