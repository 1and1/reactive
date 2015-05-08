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
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import net.oneandone.reactive.ReactiveSink;
import net.oneandone.reactive.sse.ScheduledExceutor;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.client.ChannelProvider.ChannelHandler;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;




public class ClientSseSink extends SseEndpoint implements Subscriber<ServerSentEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ClientSseSink.class);
    
    private final static Duration DEFAULT_KEEP_ALIVE_PERIOD = Duration.ofSeconds(40);
    
    private final AtomicReference<SseOutboundStream> sseOutboundStreamRef = new AtomicReference<>();
    
    private final URI uri;
    private final boolean isAutoId;
    private boolean isFailOnConnectError;
    private final Optional<Duration> connectionTimeout;
    private final int numFollowRedirects;
    private final Duration keepAlivePeriod;

    
    public ClientSseSink(URI uri) {
        this(uri, 
             true,
             DEFAULT_NUM_FOILLOW_REDIRECTS,
             true,
             Optional.empty(), 
             DEFAULT_KEEP_ALIVE_PERIOD);
    }

    private ClientSseSink(URI uri, 
                          boolean isAutoId,
                          int numFollowRedirects,
                          boolean isFailOnConnectError,
                          Optional<Duration> connectionTimeout, 
                          Duration keepAlivePeriod) {
        this.uri = uri;
        this.isAutoId = isAutoId;
        this.numFollowRedirects = numFollowRedirects;
        this.isFailOnConnectError = isFailOnConnectError;
        this.connectionTimeout = connectionTimeout;
        this.keepAlivePeriod = keepAlivePeriod;
    }
    
    public ClientSseSink autoId(boolean isAutoId) {
        return new ClientSseSink(this.uri, 
                                 isAutoId,
                                 this.numFollowRedirects,
                                 this.isFailOnConnectError,
                                 this.connectionTimeout,
                                 this.keepAlivePeriod);
    }


    public ClientSseSink connectionTimeout(Duration connectionTimeout) {
        return new ClientSseSink(this.uri,
                                 this.isAutoId, 
                                 this.numFollowRedirects,
                                 this.isFailOnConnectError,
                                 Optional.of(connectionTimeout), 
                                 this.keepAlivePeriod);
    }

    /**
     * @param keepAlivePeriod  the keep alive period 
     * @return a new instance with the updated behavior
     */
    public ClientSseSink keepAlivePeriod(Duration keepAlivePeriod) {
        return new ClientSseSink(this.uri, 
                                 this.isAutoId,
                                 this.numFollowRedirects,
                                 this.isFailOnConnectError,
                                 this.connectionTimeout,
                                 keepAlivePeriod);
    }

    
    /**
     * @param isFailOnConnectError true, if connect should fail on connect errors
     * @return a new instance with the updated behavior
     */
    public ClientSseSink failOnConnectError(boolean isFailOnConnectError) {
        return new ClientSseSink(this.uri, 
                                 this.isAutoId,
                                 this.numFollowRedirects,
                                 isFailOnConnectError,
                                 this.connectionTimeout,
                                 this.keepAlivePeriod);
    }

    
    /**
     * @param isFollowRedirects true, if redirects should be followed
     * @return a new instance with the updated behavior
     */
    public ClientSseSink followRedirects(boolean isFollowRedirects) {
        return new ClientSseSink(this.uri, 
                                 this.isAutoId,
                                 isFollowRedirects ? DEFAULT_NUM_FOILLOW_REDIRECTS : 0,
                                 this.isFailOnConnectError,
                                 this.connectionTimeout,
                                 this.keepAlivePeriod);
    }
    

    
    public ReactiveSink<ServerSentEvent> open() {
        try {
            return openAsync().get();
        } catch (InterruptedException e) {
            throw new ConnectException(e);
        } catch (ExecutionException e) {
            throw new ConnectException(e.getCause());
        }
    }
    
    
    /**
     * @return the new source instance future
     */
    public CompletableFuture<ReactiveSink<ServerSentEvent>> openAsync() {
        return ReactiveSink.subscribeAsync(this);
    }
    

    @Override
    public void onSubscribe(Subscription subscription) {
        sseOutboundStreamRef.set(new SseOutboundStream(subscription,
                                                       uri,
                                                       isAutoId,
                                                       numFollowRedirects,
                                                       isFailOnConnectError,
                                                       connectionTimeout, 
                                                       keepAlivePeriod));
        
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
        private final Subscription subscription;
        
        private final URI uri;
        private final Optional<Duration> connectionTimeout;
        private boolean isFailOnConnectError;
        private final int numFollowRedirects;

        
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        
        private final String globalId = newGlobalId();
        private final AtomicLong nextLocalId = new AtomicLong(1);
        
        private final ChannelProvider streamProvider = NettyBasedChannelProvider.newStreamProvider();
        private final AtomicReference<ChannelProvider.Stream> outboundStreamRef = new AtomicReference<>(new ChannelProvider.NullChannel());

        
        
        public SseOutboundStream(Subscription subscription, 
                                 URI uri, 
                                 boolean isAutoId,
                                 int numFollowRedirects,
                                 boolean isFailOnConnectError,
                                 Optional<Duration> connectionTimeout,
                                 Duration keepAlivePeriod) {
            
            this.subscription = subscription;
            this.isAutoId = isAutoId;
            this.uri = uri;
            this.numFollowRedirects = numFollowRedirects;
            this.isFailOnConnectError = isFailOnConnectError;
            this.connectionTimeout = connectionTimeout;
            
            
            LOG.debug("[" + id + "] opening");
            
            
            newChannelAsync(Duration.ZERO)
                          .whenComplete((stream, error) ->  {
                                                              if (error == null) {
                                                                  LOG.debug("[" + id + "] opened");
                                                                  outboundStreamRef.set(stream);
                                                                  
                                                                  // init message
                                                                  new KeepAliveEmitter(keepAlivePeriod).start();
                                                                  subscription.request(1);
                                                              } else {
                                                                  terminate(error);
                                                              }
                                                            });
        }
        
        
        
        
        private CompletableFuture<ChannelProvider.Stream> newChannelAsync(Duration retryDelay) {
            if (isOpen.get()) {
                LOG.debug("[" + id + "] open underlying channel ");
                
                ChannelHandler handler = new ChannelHandler() {
                  
                    @Override
                    public void onError(int chanelId, Throwable error) {
                        LOG.debug("error occured. reset underlying channel", error);
// TODO implement reset                        
                    }
                };
                                

                // [why not using Expect: 100-continue]
                // Unfortunately Tomcat sends the 100 continue response before passing control to the servlet. 
                // This means if the servlet sends a redirect, the 100-continue response will be alreday received
                
                return streamProvider.openChannelAsync(id, 
                                                      uri,
                                                      "POST",
                                                      ImmutableMap.of("Content-Type", "text/event-stream", "Transfer-Encoding", "chunked"),
                                                      isFailOnConnectError,
                                                      numFollowRedirects,
                                                      handler,
                                                      connectionTimeout);
                
            } else {
                return CompletableFuture.completedFuture(new ChannelProvider.NullChannel());
            }
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
            if (event.isSystem()) {
                LOG.debug("[" + id + "] sending system event " + event.toString().trim());
            } else  {
                LOG.debug("[" + id + "] sending event " + event.getId().orElse(""));
            }
            
            outboundStreamRef.get().writeAsync(event.toWire())
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
                    outboundStreamRef.getAndSet(new ChannelProvider.NullChannel()).close();
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
        
        
        
        
        /**
         * sents keep alive messages to keep the http connection alive in case of idling
         * @author grro
         */
        private final class KeepAliveEmitter {
            private final DecimalFormat formatter = new DecimalFormat("#.#");
            private final Instant start = Instant.now(); 
            private final ScheduledExecutorService executor = ScheduledExceutor.common();
            private final Duration keepAlivePeriod;
            
            
            public KeepAliveEmitter(Duration keepAlivePeriod) {
                this.keepAlivePeriod = keepAlivePeriod;
            }
            
            public void start() {
                writeAsync(ServerSentEvent.newEvent().comment("start client event streaming (keep alive period=" + keepAlivePeriod.getSeconds() + " sec)"));
                executor.schedule(() -> scheduleNextKeepAliveEvent(), keepAlivePeriod.getSeconds(), TimeUnit.SECONDS);
            }
            
            private void scheduleNextKeepAliveEvent() {
                if (isOpen.get()) {
                    writeAsync(ServerSentEvent.newEvent().comment("keep alive from client (age " + format(Duration.between(start, Instant.now())) + ")"))
                        .thenAccept(numWritten -> executor.schedule(() -> scheduleNextKeepAliveEvent(), keepAlivePeriod.getSeconds(), TimeUnit.SECONDS));
                }
            }       
            
            private CompletableFuture<Void> writeAsync(ServerSentEvent event) {
                LOG.debug("[" + id + "] writing system event " + event.toString().trim());
                return outboundStreamRef.get().writeAsync(event.toWire());
            }
            
            private String format(Duration duration) {
                if (duration.getSeconds() > (60 * 60)) {
                    return formatter.format(((float) duration.getSeconds() / (60 * 60))) + " hours";
                } else if (duration.getSeconds() > 120) {
                    return formatter.format(((float) duration.getSeconds() / 60)) + " min";
                } else {
                    return duration.getSeconds() + " sec";
                }
            }
        } 
    }
}