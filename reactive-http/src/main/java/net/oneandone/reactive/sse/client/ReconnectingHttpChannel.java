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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import net.oneandone.reactive.sse.client.HttpChannelProvider.ConnectionParams;
import net.oneandone.reactive.sse.client.HttpChannelDataHandler;
import net.oneandone.reactive.sse.client.HttpChannel;
import net.oneandone.reactive.utils.RetryScheduler;
import net.oneandone.reactive.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;




class ReconnectingHttpChannel implements HttpChannel {
    private static final Logger LOG = LoggerFactory.getLogger(ReconnectingHttpChannel.class);
    private static final HttpChannel NULL_CHANNEL = new HttpChannel.NullHttpChannel();
    
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
    private final AtomicBoolean isReadSuspended = new AtomicBoolean(false);
    private final AtomicReference<HttpChannel> channelRef = new AtomicReference<>(NULL_CHANNEL);
    private final HttpChannelProvider channelProvider = HttpChannelProviderFactory.newHttpChannelProvider();
    private final RetryScheduler retryProcessor = new RetryScheduler();
    private final Object reconnectingLock = new Object();
    private boolean isReconnecting = false;

    
    // statistics
    private final AtomicInteger numReconnectTrials = new AtomicInteger();
    private final AtomicInteger numReconnects = new AtomicInteger();
    
   
    
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
    public String getId() {
        return getCurrentHttpChannel().getId();
    }
    
    
    @Override
    public boolean isOpen() {
        return getCurrentHttpChannel().isOpen();
    }
  
    @Override
    public boolean isReadSuspended() {
        return isReadSuspended.get();
    }

    @Override
    public void suspendRead(boolean isSuspended) {
        // [sync] modifying isReadSuspended property and channel has to be atomic
        // to avoid race conditions by updating isReadSuspended property and channel(ref)
        // in parallel
        synchronized (channelRef) {
            isReadSuspended.set(isSuspended);
            getCurrentHttpChannel().suspendRead(isSuspended);
        }
    }
    
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        
        if (!isOpen.get()) {
            builder.append("[closed] " + id);
            
        } else if (getCurrentHttpChannel().isOpen()) {
            builder.append((getCurrentHttpChannel().isReadSuspended() ? "[suspended] " : "") + getId());
                
        } else {
            builder.append("[not connected] " + id);
        }
        
        builder.append(" numReconnectTrials: " + numReconnectTrials.get() + ", numReconnects: " + numReconnects.get());
        
        return builder.toString();
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
            return CompletableFuture.completedFuture(NULL_CHANNEL);
        }
    }
    
  
    
    @Override
    public void terminate() {
        channelRef.get().terminate();  // terminate -> end chunk should NOT be written (refer chunked-transfer encoding)
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
    
    
    
    private void closeCurrentChannel(Throwable error) {
        // replace current one with null channel (closes old implicitly)
        replaceCurrentChannel(NULL_CHANNEL);          
        
        // notify close of former channel (-> reset sse parser)
        dataHandler.onError(id, (error == null) ? new ClosedChannelException() : error);
    }        
    
    
    private void replaceCurrentChannel(HttpChannel channel) {
     
        // [sync] modifying isReadSuspended property and channel has to be atomic
        // to avoid race conditions by updating isReadSuspended property and channel(ref)
        // in parallel
        synchronized (channelRef) {
            // restore suspend state 
            channel.suspendRead(isReadSuspended());
            
            // close the old channel and reset it by the new one
            channelRef.getAndSet(channel).close();
        }
        
        isWriteableStateChangedListener.accept(true);
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
    
                    numReconnectTrials.incrementAndGet();
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
                                                                        numReconnects.incrementAndGet();
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
}
