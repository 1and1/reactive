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

import net.oneandone.reactive.ConnectException;
import net.oneandone.reactive.sse.ScheduledExceutor;
import net.oneandone.reactive.sse.client.StreamProvider.DataConsumer;
import net.oneandone.reactive.sse.client.StreamProvider.StreamHandler;
import net.oneandone.reactive.sse.client.StreamProvider.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;







import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
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
    private final DataConsumer dataConsumer;
    private final Consumer<Boolean> isWriteableStateChangedListener;
    private final Function<ImmutableMap<String, String>, ImmutableMap<String, String>> headerInterceptor;

   
    // underlying stream
    private final StreamManager streamManager = new StreamManager();
    private final StreamProvider channelProvider;
    
    
   
    
    public ReconnectingStream(String id, 
                             URI uri, 
                             String method,
                             ImmutableMap<String, String> headers,
                             boolean isFailOnConnectError,
                             int numFollowRedirects,
                             Optional<Duration> connectionTimeout,
                             Consumer<Boolean> isWriteableStateChangedListener,
                             DataConsumer dataConsumer,
                             Function<ImmutableMap<String, String>, ImmutableMap<String, String>> headerInterceptor) {
        this.id = id;
        this.uri = uri;
        this.method = method;
        this.headers = headers;
        this.numFollowRedirects = numFollowRedirects;
        this.isFailOnConnectError = isFailOnConnectError;
        this.connectionTimeout = connectionTimeout;
        this.isWriteableStateChangedListener = isWriteableStateChangedListener;
        this.dataConsumer = dataConsumer;
        this.headerInterceptor = headerInterceptor;
        
        this.channelProvider = NettyBasedChannelProvider.newStreamProvider();
    }

    
    public CompletableFuture<Boolean> init() {
        
        return newStreamAsync()
                    .thenApply(stream -> { 
                                            LOG.debug("[" + id + "] initially connected");
                                            streamManager.onConnected(stream); 
                                            return true;
                                         })
                                                    
                    .exceptionally(error -> { 
                                                 // initial "connect" failed
                                                 if (isFailOnConnectError) {
                                                     LOG.debug("[" + id + "] initial connect failed. " + error.getMessage());
                                                     throw new ConnectException(error);
                                                     
                                                 // initial "connect" failed, however should be ignored
                                                 } else { 
                                                     LOG.debug("[" + id + "] initial connect failed. " + error.getMessage() + " Trying to reconnect");
                                                     streamManager.reconnect();
                                                     return false;
                                                 }
                                            });
    }

    
 
    public CompletableFuture<Void> writeAsync(String data) {
        return streamManager.getStream()
                            .writeAsync(data)
                            .exceptionally(error -> { 
                                                        streamManager.reconnect(); 
                                                        throw Throwables.propagate(error); 
                                                    });
    }
    
    
    @Override
    public String getStreamId() {
        return id + "/" + streamManager.getStream().getStreamId();
    }
    
    
    @Override
    public boolean isConnected() {
        return streamManager.getStream().isConnected();
    }
    
    
    @Override
    public void terminate() {
        streamManager.terminate();
        close();
    }
    
    
    @Override
    public void close() {
        streamManager.closeStream();
            
        if (isOpen.getAndSet(false)) {
            LOG.debug("[" + id + "] close");
            channelProvider.closeAsync();
        }
    } 
   

    @Override
    public boolean isReadSuspended() {
        return streamManager.getStream().isReadSuspended();
    }

    
    @Override
    public void resumeRead() {
        streamManager.getStream().resumeRead();
    }
    
    
    @Override
    public void suspendRead() {
        streamManager.getStream().suspendRead();
    }
     
    
    @Override
    public String toString() {
        if (!isOpen.get()) {
            return "[closed] " + id;
            
        } else if (streamManager.getStream().isConnected()) {
            return (streamManager.getStream().isReadSuspended() ? "[suspended] " : "") + getStreamId();
                
        } else {
            return "[not connected] " + id;
        }
    }
    
          

    
    
    private final class StreamManager {
        private final RetryScheduler retryProcessor = new RetryScheduler();
        private final AtomicReference<StreamProvider.Stream> streamRef = new AtomicReference<>(new StreamProvider.NullStream(false));

        private final Object reconnectLock = new Object(); 
        private boolean isAlreadyRunning = false;
        
    
        
        public Stream getStream() {
            return streamRef.get();
        }

        
        public void terminate() {
            synchronized (reconnectLock) {
                dataConsumer.onReset();
                streamRef.getAndSet(new StreamProvider.NullStream(true)).terminate();  // terminate -> end chunk should NOT be written (refer chunked-transfer encoding)
            }
        }

        public void closeStream() {
            synchronized (reconnectLock) {
                if (streamRef.get().isConnected()) {
                    LOG.debug("[" + id + "] close underlying stream");
                }
                onConnected(new StreamProvider.NullStream(true));
            }
        }
        
        
        public void onConnected(Stream stream) {
            synchronized (reconnectLock) {
                // close old stream
                Stream oldStream = streamRef.get();
                oldStream.close();

                // reset data consumer
                dataConsumer.onReset();
                
                // restore suspend state for new stream
                if (oldStream.isReadSuspended()) {
                    stream.suspendRead();
                } else {
                    stream.resumeRead();
                }
                
                // set new stream
                streamRef.set(stream);
                if (stream.isConnected()) {
                    isWriteableStateChangedListener.accept(true);
                }
            }
        }        
        
        
        private void reconnect() {
            synchronized (reconnectLock) {
                closeStream();
        
                if (isOpen.get()) {
                    performReconnect();
                }
            }
        }

        
        
        private void performReconnect() {

            synchronized (reconnectLock) {
     
                if (!isAlreadyRunning) {
                    isAlreadyRunning = true;

                    Runnable retryConnect = () -> {
                        newStreamAsync()
                            .whenComplete((stream, error) -> { 
                                                                synchronized (reconnectLock) {
                                                                    isAlreadyRunning = false;
                                                                    
                                                                    if (error == null) {
                                                                        if (stream.isConnected()) {
                                                                            LOG.debug("[" + id + "] stream reconnected");
                                                                        }
                                                                        onConnected(stream);
                                                                        
                                                                    } else {
                                                                        LOG.debug("[" + id + "] stream reconnect failed. Trying again");
                                                                        performReconnect();
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
    
    
    
    
    
    private CompletableFuture<StreamProvider.Stream> newStreamAsync() { 
       
        if (isOpen.get()) {
            // process headers interceptor
            ImmutableMap<String, String> additionalHeaders = headerInterceptor.apply(headers);
            
            LOG.debug("[" + id + "] open underlying stream (" + method + " " + uri + " - " + Joiner.on("&").withKeyValueSeparator("=").join(additionalHeaders) + ")");
            
            StreamHandler handler = new StreamHandler() {
              
                @Override
                public Optional<StreamHandler> onContent(int channelId, ByteBuffer[] buffers) {
                    dataConsumer.onData(buffers); 
                    return Optional.empty();
                }
                
                @Override
                public void onError(int channelId, Throwable error) {
                    LOG.debug("[" + id + "] - " + channelId + " error " + error.getMessage() + " occured. Trying to reconnect");
                    streamManager.reconnect();
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
            return CompletableFuture.completedFuture(new StreamProvider.NullStream(true));
        }
    }
    
    
    

    private static final class RetryScheduler {
        private static final RetrySequence RETRY_SEQUENCE = new RetrySequence(0, 25, 250, 500, 2000, 3000, 4000, 6000);

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
