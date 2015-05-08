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


import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



class NettyBasedChannelProvider implements ChannelProvider {
    private static final Logger LOG = LoggerFactory.getLogger(NettyBasedChannelProvider.class);
    
    private final EventLoopGroup eventLoopGroup;
    
    private NettyBasedChannelProvider() {
        eventLoopGroup = new NioEventLoopGroup();
    }
    
    
    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        
        eventLoopGroup.shutdownGracefully().addListener(new FutureListener<Object>() {
         
            @Override
            public void operationComplete(Future<Object> future) throws Exception {
                 if (future.isSuccess()) {
                     promise.complete(null);
                 } else {
                     promise.completeExceptionally(future.cause());
                 }
            }
        });
        
        return promise;
    }
    
    
    @Override
    public CompletableFuture<Stream> openChannelAsync(String id,
                                                     URI uri, 
                                                     String method, 
                                                     ImmutableMap<String, String> headers, 
                                                     boolean isFailOnConnectError,
                                                     int numFollowRedirects,
                                                     ChannelHandler handler,
                                                     Optional<Duration> connectTimeout) {
        return connect(id, 
                       uri, 
                       HttpMethod.valueOf(method), 
                       headers, 
                       connectTimeout, 
                       numFollowRedirects,
                       handler);
    }
    
    

    
    private CompletableFuture<Stream> connect(String id,
                                              URI uri,
                                              HttpMethod method, 
                                              ImmutableMap<String, String> headers,
                                              Optional<Duration> connectTimeout, 
                                              int numFollowRedirects,
                                              ChannelHandler streamHandler) {
    
        CompletableFuture<Stream> promise = new CompletableFuture<>();
        
        
        connect(id, uri, method, headers, connectTimeout, streamHandler)
            .whenComplete((stream, error) -> { 
                                                if (error == null) {
                                                    promise.complete(stream);
                                                } else {
                                                    if ((numFollowRedirects > 0) && (error instanceof HttpResponseError)) {
                                                        Optional<URI> redirectURI = ((HttpResponseError) error).getRedirectLocation();
                                                        if (redirectURI.isPresent()) {
                                                            LOG.debug("[" + id + "] follow redirect " + redirectURI.get());
                                                            connect(id, redirectURI.get(), method, headers, connectTimeout, numFollowRedirects - 1, streamHandler)
                                                                    .whenComplete((stream2, error2) -> { 
                                                                                                            if (error2 == null) {
                                                                                                                promise.complete(stream2);
                                                                                                            } else {
                                                                                                                promise.completeExceptionally(error2); 
                                                                                                            }
                                                                                                       });
                                                        } else {
                                                            promise.completeExceptionally(error);
                                                        }
                                                    } else {
                                                        promise.completeExceptionally(error);
                                                    }
                                                }
                                             });
        return promise;
    }
    
    
    
    
    
    private CompletableFuture<Stream> connect(String id,
                                              URI uri,
                                              HttpMethod method, 
                                              ImmutableMap<String, String> headers,
                                              Optional<Duration> connectTimeout, 
                                              ChannelHandler streamHandler) {
        
        ConnectedPromise connectedPromise = new ConnectedPromise(id, streamHandler);
        
        try {
            String scheme = (uri.getScheme() == null) ? "http" : uri.getScheme();
            boolean ssl = "https".equalsIgnoreCase(scheme);
            SslContext sslCtx;
            if (ssl) {
                sslCtx = SslContext.newClientContext();
            } else {
                sslCtx = null;
            }
    
            
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                     .channel(NioSocketChannel.class)
                     .handler(new HttpChannelInitializer(id, sslCtx, connectedPromise));
            
            if (connectTimeout.isPresent()) {
                bootstrap = bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.get().toMillis());
            }
    
    
            String host = uri.getHost(); 
            int p = uri.getPort();
            if (p == -1) {
                if ("http".equalsIgnoreCase(scheme)) {
                    p = 80;
                } else if ("https".equalsIgnoreCase(scheme)) {
                    p = 443;
                }
            }
            int port = p; 
            
            
            LOG.debug("[" + id + "] - opening channel with "  + bootstrap.toString());
            
            ChannelFutureListener listener = new ChannelFutureListener() {
    
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        LOG.debug("[" + id + "] - channel " + future.channel().hashCode() + " opened. Sending GET request header " + uri.getRawPath() + ((uri.getRawQuery() == null) ? "" : "?" + uri.getRawQuery()));
                        
                        DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, uri.getRawPath() + ((uri.getRawQuery() == null) ? "" : "?" + uri.getRawQuery()));
                        request.headers().set(HttpHeaders.Names.HOST, host + ":" + port);
                        request.headers().set(HttpHeaders.Names.USER_AGENT, "sseclient/1.0");
                        headers.forEach((name, value) -> request.headers().set(name, value));
                        
                        future.channel().writeAndFlush(request);
                    } else {
                        connectedPromise.onError(future.channel().hashCode(), future.cause());
                    }
                }
            };
            bootstrap.connect(host, port).addListener(listener);
            
            
        } catch (SSLException | RuntimeException e) {
            connectedPromise.onError(-1, new ConnectException("could not connect to " + uri, e));
        }
        
        return connectedPromise;
    }
    
    
    
    private static class ConnectedPromise extends CompletableFuture<Stream> implements ChannelHandler {
        private final ChannelProvider.ChannelHandler dataHandler;
        private final String id;
        
        public ConnectedPromise(String id, ChannelProvider.ChannelHandler dataHandler) {
            this.id = id;
            this.dataHandler = dataHandler;
        }
        
        public ChannelHandler onResponseHeader(Channel channel, HttpResponse response) {
            LOG.debug("[" + id + "] - channel " + channel.hashCode() + " got response " + response.getStatus().code());
            complete(new Http11Stream(id, channel));
            
            return dataHandler;
        }
        
        @Override
        public void onError(int channelId, Throwable error) {
            LOG.debug("[" + id + "] - channel " + channelId + " error occured " + error.getMessage());
            completeExceptionally(error);
        }
        
        @Override
        public Optional<ChannelHandler> onContent(int channelId, ByteBuffer[] buffers) {
            return Optional.empty();
        }
    }

    
    
    private static class Http11Stream implements Stream  {
        private final String id; 
        private final Channel channel; 
        
        public Http11Stream(String id, Channel channel) {
            this.id = id;
            this.channel = channel;
        }
        
        
        @Override
        public CompletableFuture<Void> writeAsync(String msg) {
            CompletableFuture<Void> promise = new CompletableFuture<>();

            ChannelFuture future = channel.writeAndFlush(new DefaultHttpContent(Unpooled.copiedBuffer(msg, StandardCharsets.UTF_8)));

            ChannelFutureListener listener = new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        promise.complete(null);
                    } else {
                        promise.completeExceptionally(future.cause());
                    }
                }
            };
            future.addListener(listener);
    
            
            return promise;
        }
        
        
        @Override
        public boolean isReadSuspended() {
            return !channel.config().isAutoRead();
        }
        
        @Override
        public void suspendRead() {
            LOG.debug("[" + id + "] - channel " + channel.hashCode() + " suspended");
            channel.config().setAutoRead(false);
        }
        
        @Override
        public void resumeRead() {
            LOG.debug("[" + id + "] - channel " + channel.hashCode() + " resumed");
            channel.config().setAutoRead(true);
        }
        
        @Override
        public void terminate() {
            channel.close();
        }

        @Override
        public void close() {
            channel.close();
        }
        
        @Override
        public boolean isConnected() {
            return channel.isOpen();
        }
    }


    
    
    
   
    private static final ChannelProvider COMMON = new NettyBasedChannelProvider();
    
    public static ChannelProvider newStreamProvider() {
        return new StreamProviderHandle();
    } 
    
    private static final class StreamProviderHandle implements ChannelProvider {
        private final ChannelProvider delegate;
        
        
        public StreamProviderHandle() {
            delegate = COMMON;
        }
        
        @Override
        public CompletableFuture<Stream> openChannelAsync(String id,
                                                         URI uri,
                                                         String method, 
                                                         ImmutableMap<String, String> headers,
                                                         boolean isFailOnConnectError, 
                                                         int numFollowRedirects,
                                                         ChannelHandler handler,
                                                         Optional<Duration> connectTimeout) {
            return delegate.openChannelAsync(id, 
                                            uri, 
                                            method,
                                            headers,
                                            isFailOnConnectError, 
                                            numFollowRedirects,
                                            handler,
                                            connectTimeout);
        }
        
        
        
        @Override
        public CompletableFuture<Void> closeAsync() {
            return CompletableFuture.completedFuture(null);
        }
    }
    
    
      
   
    
    
    private static final class HttpResponseError extends RuntimeException {
        private static final long serialVersionUID = 5524737399197875355L;        
        private static final ImmutableSet<Integer> REDIRECT_STATUS_CODES = ImmutableSet.of(301, 302, 303, 307);


        private final HttpResponse response;
        
        public HttpResponseError(HttpResponse response) {   
            super(response.getStatus() + " response received");
            this.response = response; 
        }

        public Optional<URI> getRedirectLocation() {
            if (REDIRECT_STATUS_CODES.contains(response.getStatus().code())) {
                return Optional.ofNullable(URI.create(response.headers().get("Location")));
            } else {
                return Optional.empty();
            }
        }
    }
    
    
    private static class HttpChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final String id;
        private final SslContext sslCtx;
        private final ChannelHandler channelHandler;

        public HttpChannelInitializer(String id,
                                      SslContext sslCtx,
                                      ChannelHandler channelHandler) {
            this.id = id;
            this.sslCtx = sslCtx;
            this.channelHandler = channelHandler;
        }
                
        
        @Override
        public void initChannel(SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();
             
             if (sslCtx != null) {
                 p.addLast(sslCtx.newHandler(ch.alloc()));
             }

             p.addLast(new HttpClientCodec());
  
             p.addLast(new HttpContentDecompressor());
             p.addLast(new HttpInboundHandler(id, channelHandler));
        }
    }
    
    
    private static class HttpInboundHandler extends SimpleChannelInboundHandler<HttpObject> {
        private final String id;
        private final Instant start = Instant.now();
        private final AtomicReference<ChannelProvider.ChannelHandler> channelHandlerRef;

        public HttpInboundHandler(String id, ChannelHandler channelHandler) {
            this.id = id;
            this.channelHandlerRef = new AtomicReference<>(channelHandler);
        }
                
        
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {

            if (msg instanceof HttpResponse) {
                HttpResponse response = (HttpResponse) msg;
                int status = response.getStatus().code();
                LOG.debug("[" + id + "] - channel " + ctx.channel().hashCode() + " response " + status +  " received");

                if ((status / 100) == 2) {
                    ChannelHandler dataHandler = channelHandlerRef.get().onResponseHeader(ctx.channel(), response);
                    channelHandlerRef.set(dataHandler);
                } else {
                    notifyError(ctx, new HttpResponseError(response));
                }
                
            } else  if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;
                
                Optional<ChannelHandler> newDataHandler = channelHandlerRef.get().onContent(ctx.channel().hashCode(), content.content().nioBuffers());
                newDataHandler.ifPresent(handler -> channelHandlerRef.set(handler));
                
                if (content instanceof LastHttpContent) {
                    ctx.close();
                }
            }
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) throws Exception {
            notifyError(ctx, error);
        }
        
        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            StringBuilder builder = new StringBuilder("[" + id + "] - channel " + ctx.channel().hashCode() + " is closed (channelUnregistered event)");
            if (!ctx.channel().config().isAutoRead()) {
                builder.append(" - read is suspended - ");
            }
            builder.append(" age: " + Duration.between(start, Instant.now()).getSeconds() + " sec");
            LOG.debug(builder.toString());
            
            notifyError(ctx, new RuntimeException("channel closed"));
        }
        
        
        private void notifyError(ChannelHandlerContext ctx, Throwable error) {
            channelHandlerRef.getAndSet(new ChannelProvider.ChannelHandler() { }).onError(ctx.channel().hashCode(), error);
            ctx.close();
        }   
    }
}        
