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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;



class NettyBasedStreamProvider implements StreamProvider {
    private static final Logger LOG = LoggerFactory.getLogger(NettyBasedStreamProvider.class);
    
    private final EventLoopGroup eventLoopGroup;
    
    private NettyBasedStreamProvider() {
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
    public CompletableFuture<OutboundStream> newOutboundStream(String id, 
                                                               URI uri,
                                                               Consumer<Void> closeConsumer,
                                                               Optional<Duration> connectTimeout, 
                                                               Optional<Duration> socketTimeout) {
        
        return NettyHttp11OutboundStream.openAsync(eventLoopGroup, 
                                                   id,    
                                                   uri, 
                                                   closeConsumer,
                                                   connectTimeout,
                                                   socketTimeout);
    }

 
    
    
    @Override
    public CompletableFuture<InboundStream> openInboundStreamAsync(String id,
                                                                   URI uri, 
                                                                   Optional<String> lastEventId, 
                                                                   boolean isFailOnConnectError,
                                                                   int numFollowRedirects,
                                                                   InboundStreamHandler handler,
                                                                   Optional<Duration> connectTimeout, 
                                                                   Optional<Duration> socketTimeout) {

        return connect(id, 
                uri, 
                HttpMethod.GET, 
                lastEventId.isPresent() ? ImmutableMap.of(HttpHeaders.Names.ACCEPT, "text/event-stream", "Last-Event-ID", lastEventId.get()) : ImmutableMap.of(HttpHeaders.Names.ACCEPT, "text/event-stream"), 
                connectTimeout, 
                socketTimeout, 
                numFollowRedirects,
                handler);
    }
    
    

    
    private CompletableFuture<InboundStream> connect(String id,
                                                     URI uri,
                                                     HttpMethod method, 
                                                     ImmutableMap<String, String> headers,
                                                     Optional<Duration> connectTimeout, 
                                                     Optional<Duration> socketTimeout,
                                                     int numFollowRedirects,
                                                     InboundStreamHandler streamHandler) {
    
        CompletableFuture<InboundStream> promise = new CompletableFuture<>();
        
        
        connect(id, uri, method, headers, connectTimeout, socketTimeout, streamHandler)
            .whenComplete((stream, error) -> { 
                                                if (error == null) {
                                                    promise.complete(stream);
                                                } else {
                                                    if ((numFollowRedirects > 0) && (error instanceof HttpResponseError)) {
                                                        Optional<URI> redirectURI = ((HttpResponseError) error).getRedirectLocation();
                                                        if (redirectURI.isPresent()) {
                                                            LOG.debug("[" + id + "] follow redirect " + redirectURI.get());
                                                            connect(id, redirectURI.get(), method, headers, connectTimeout, socketTimeout, numFollowRedirects - 1, streamHandler)
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
    
    
    
    
    
    private CompletableFuture<InboundStream> connect(String id,
                                                     URI uri,
                                                     HttpMethod method, 
                                                     ImmutableMap<String, String> headers,
                                                     Optional<Duration> connectTimeout, 
                                                     Optional<Duration> socketTimeout,
                                                     InboundStreamHandler streamHandler) {
        
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
                     .handler(new InboundHttpChannelInitializer(id, sslCtx, connectedPromise));
            
            if (connectTimeout.isPresent()) {
                bootstrap = bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.get().toMillis());
            }
            if (socketTimeout.isPresent()) {
                bootstrap = bootstrap.option(ChannelOption.SO_TIMEOUT, (int) socketTimeout.get().toMillis());
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
                    }
                }
            };
            bootstrap.connect(host, port).addListener(listener);
            
        } catch (SSLException | RuntimeException e) {
            connectedPromise.onError(e);
        }
        
        return connectedPromise;
    }
    
    
    
    private static class ConnectedPromise extends CompletableFuture<InboundStream> implements HttpResponseHandler {
        private final StreamProvider.InboundStreamHandler streamHandler;
        private final String id;
        private final AtomicBoolean isHandled = new AtomicBoolean(false);
        
        public ConnectedPromise(String id, StreamProvider.InboundStreamHandler streamHandler) {
            this.id = id;
            this.streamHandler = streamHandler;
        }
        
        @Override
        public StreamProvider.InboundStreamHandler onConnect(Channel channel) {
            if (!isHandled.getAndSet(true)) {
                complete(new Http11InboundStream(id, channel));
            }
            return streamHandler;
        }
            
        @Override
        public void onError(Throwable error) {
            if (!isHandled.getAndSet(true)) {
                completeExceptionally(error);
            }
        }
    }

    
    
    private static class Http11InboundStream implements InboundStream  {
        private final String id; 
        private final Channel channel; 
        
        public Http11InboundStream(String id, Channel channel) {
            this.id = id;
            this.channel = channel;
        }
        
        @Override
        public boolean isSuspended() {
            return !channel.config().isAutoRead();
        }
        
        @Override
        public void suspend() {
            LOG.debug("[" + id + "] - channel " + channel.hashCode() + " suspended");
            channel.config().setAutoRead(false);
        }
        
        @Override
        public void resume() {
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
    }


    
    
    
    private static class NettyHttp11OutboundStream implements OutboundStream  {
        private static final Logger LOG = LoggerFactory.getLogger(NettyHttp11OutboundStream.class);
        
        
        static CompletableFuture<OutboundStream> openAsync(EventLoopGroup eventLoopGroup,
                                                           String id,
                                                           URI uri, 
                                                           Consumer<Void> closeConsumer, 
                                                           Optional<Duration> connectTimeout, 
                                                           Optional<Duration> socketTimeout) {
            
            CompletableFuture<OutboundStream> promise = new CompletableFuture<>();
            
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
                         .handler(new OutboundHttpChannelInitializer(id, sslCtx, closeConsumer));
                
                if (connectTimeout.isPresent()) {
                    bootstrap = bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeout.get().toMillis());
                }
                if (socketTimeout.isPresent()) {
                    bootstrap = bootstrap.option(ChannelOption.SO_TIMEOUT, (int) socketTimeout.get().toMillis());
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
                
                
                ChannelFutureListener listener = new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            LOG.debug("[" + id + "] - channel " + future.channel().hashCode() + " opened");
                            writeRequestHeaderAsync(future.channel(), uri, host, port)
                                             .whenComplete((Void, error) -> { if (error == null) {
                                                                                     LOG.debug("[" + id + "] - channel " + future.channel().hashCode() + " POST request header sent");
                                                                                     promise.complete(new NettyHttp11OutboundStream(id, future.channel())); 
                                                                              } else {
                                                                                  promise.completeExceptionally(error);
                                                                              }
                                                                            });
                        } else {
                            promise.completeExceptionally(future.cause());
                        }
                    }
                };
                bootstrap.connect(host, port).addListener(listener); 
                
            } catch (SSLException e) {
                promise.completeExceptionally(e);
            }
            
            return promise;
        }
        
        
        
        
        private static CompletableFuture<Void> writeRequestHeaderAsync(Channel channel, URI uri, String host, int port) {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            
            DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri.getRawPath());
            request.headers().set(HttpHeaders.Names.HOST, host + ":" + port);
            request.headers().set(HttpHeaders.Names.USER_AGENT, "sseclient/1.0");
            request.headers().set(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
            

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
            channel.writeAndFlush(request).addListener(listener);
            
            return promise;
        }
        
        
        
        

        private final String id;
        private final Channel channel;
        

        public NettyHttp11OutboundStream(String id, Channel channel) {
            this.id = id;
            this.channel = channel;
        }
        
                
        
        public CompletableFuture<Void> write(String msg) {
            ChannelFuture future = channel.writeAndFlush(new DefaultHttpContent(Unpooled.copiedBuffer(msg, StandardCharsets.UTF_8)));
            
            CompletableFuture<Void> promise = new CompletableFuture<>();
            
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
        public void close() {
            channel.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            closeChannel();
        }
    
        public void terminate() {
            closeChannel();
        }
        
        private void closeChannel() {
            channel.close();
        }
        
        
        private static class OutboundHttpChannelInitializer extends ChannelInitializer<SocketChannel> {
            private final String id;
            private final SslContext sslCtx;
            private final Consumer<Void> closeConsumer;
            
            public OutboundHttpChannelInitializer(String id, SslContext sslCtx, Consumer<Void> closeConsumer) {
                this.id = id;
                this.sslCtx = sslCtx;
                this.closeConsumer = closeConsumer;
            }
            
            @Override
            public void initChannel(SocketChannel ch) {
                 ChannelPipeline p = ch.pipeline();
                 
                 if (sslCtx != null) {
                     p.addLast(sslCtx.newHandler(ch.alloc()));
                 }
    
                 p.addLast(new HttpClientCodec());
      
                 p.addLast(new HttpContentDecompressor());
                 p.addLast(new HttpInboundHandler(id, closeConsumer));
            }
        }
    
        
        private static class HttpInboundHandler extends SimpleChannelInboundHandler<HttpObject> {
            private final String id;
            private final Consumer<Void> closeConsumer;

            
            public HttpInboundHandler(String id, Consumer<Void> closeConsumer) {
                this.id = id;
                this.closeConsumer = closeConsumer;
            }
            
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
                if (msg instanceof HttpResponse) {
                    HttpResponse response = (HttpResponse) msg;
                    
                } else  if (msg instanceof HttpContent) {
                    HttpContent content = (HttpContent) msg;
                    
                    if (content instanceof LastHttpContent) {
                        ctx.close();
                        closeConsumer.accept(null);
                    }
                }
            }
            
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                super.exceptionCaught(ctx, cause);
            }
        }
    }
    
    
    
    
   
    private static final StreamProvider COMMON = new NettyBasedStreamProvider();
    
    public static StreamProvider newStreamProvider() {
        return new StreamProviderHandle();
    } 
    
    private static final class StreamProviderHandle implements StreamProvider {
        private final StreamProvider delegate;
        
        
        public StreamProviderHandle() {
            delegate = COMMON;
        }
        
        @Override
        public CompletableFuture<OutboundStream> newOutboundStream(String id,
                                                                   URI uri, 
                                                                   Consumer<Void> closeConsumer,
                                                                   Optional<Duration> connectTimeout,
                                                                   Optional<Duration> socketTimeout) {
            return delegate.newOutboundStream(id, 
                                              uri, 
                                              closeConsumer,
                                              connectTimeout,
                                              socketTimeout);
        }
        

        @Override
        public CompletableFuture<InboundStream> openInboundStreamAsync(String id,
                                                                       URI uri,
                                                                       Optional<String> lastEventId,
                                                                       boolean isFailOnConnectError,
                                                                       int numFollowRedirects,
                                                                       InboundStreamHandler handler,  
                                                                       Optional<Duration> connectTimeout,
                                                                       Optional<Duration> socketTimeout) {
            return delegate.openInboundStreamAsync(id, 
                                                   uri, 
                                                   lastEventId,
                                                   isFailOnConnectError,
                                                   numFollowRedirects,
                                                   handler, 
                                                   connectTimeout, 
                                                   socketTimeout);
        }
        
        @Override
        public CompletableFuture<Void> closeAsync() {
            return CompletableFuture.completedFuture(null);
        }
    }
    
    
    
    
    
    
    
    
    
    

  
    

    public static interface HttpResponseHandler {
        
        StreamProvider.InboundStreamHandler onConnect(Channel channel);
        
        void onError(Throwable error);
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
    
    
    private static class InboundHttpChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final String id;
        private final SslContext sslCtx;
        private final HttpResponseHandler responseHandler;

        public InboundHttpChannelInitializer(String id, SslContext sslCtx, HttpResponseHandler responseHandler) {
            this.id = id;
            this.sslCtx = sslCtx;
            this.responseHandler = responseHandler;
        }
                
        
        @Override
        public void initChannel(SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();
             
             if (sslCtx != null) {
                 p.addLast(sslCtx.newHandler(ch.alloc()));
             }

             p.addLast(new HttpClientCodec());
  
             p.addLast(new HttpContentDecompressor());
             p.addLast(new HttpInboundHandler(id, responseHandler));
        }
        
        
        
        private static class HttpInboundHandler extends SimpleChannelInboundHandler<HttpObject> {
            private final String id;
            private final HttpResponseHandler responseHandler;
            private final AtomicReference<StreamProvider.InboundStreamHandler> streamHandlerRef = new AtomicReference<>(new StreamProvider.InboundStreamHandler.Empty());

    
            public HttpInboundHandler(String id, HttpResponseHandler responseHandler) {
                this.id = id;
                this.responseHandler = responseHandler;
            }
                    
            
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {

                if (msg instanceof HttpResponse) {
                    HttpResponse response = (HttpResponse) msg;
                    int status = response.getStatus().code();
                    LOG.debug("[" + id + "] - channel " + ctx.channel().hashCode() + " response " + status +  " received");
                    
                    if ((status / 100) == 2) {
                        StreamProvider.InboundStreamHandler streamHandler = responseHandler.onConnect(ctx.channel());
                        streamHandlerRef.set(streamHandler);
                    } else {
                        responseHandler.onError(new HttpResponseError(response));
                        ctx.close();
                    }
                    
                } else  if (msg instanceof HttpContent) {
                    HttpContent content = (HttpContent) msg;
                    
                    streamHandlerRef.get().onContent(content.content().nioBuffers());
                    if (content instanceof LastHttpContent) {
                        ctx.close();
                    }
                }
            }
            
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                responseHandler.onError(cause);
                streamHandlerRef.get().onError(cause);
                ctx.close();
            }
            
            @Override
            public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
                responseHandler.onError(new RuntimeException("channel is closed"));
                streamHandlerRef.get().onCompleted();
            }
        }
    }
}        
