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

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javax.net.ssl.SSLException;



class NettyBasedStreamProvider implements StreamProvider {
    private final EventLoopGroup eventLoopGroup;
    
    private NettyBasedStreamProvider() {
        eventLoopGroup = new NioEventLoopGroup();
    }
    
        
    @Override
    public void close() {
        eventLoopGroup.shutdownGracefully();
    }
    
    
    @Override
    public CompletableFuture<InboundStream> openInboundStreamAsync(URI uri, 
                                                                   Optional<String> lastEventId, 
                                                                   Consumer<ByteBuffer[]> dataConsumer, 
                                                                   Consumer<Void> closeConsumer, 
                                                                   Consumer<Throwable> errorConsumer,
                                                                   Optional<Duration> connectTimeout, 
                                                                   Optional<Duration> socketTimeout) {
        return NettyHttp11InboundStream.openAsync(eventLoopGroup, uri, lastEventId, dataConsumer, closeConsumer, errorConsumer, connectTimeout, socketTimeout);
    }
    
    
    @Override
    public OutboundStream newOutboundStream(URI uri, Consumer<Void> closeConsumer) {
        return new NettyHttp11OutboundStream(uri, closeConsumer);
    }

    

    private static class NettyHttp11InboundStream implements InboundStream  {
        private final Channel channel;
        private final AtomicBoolean isSuspended = new AtomicBoolean(false);

        
        static CompletableFuture<InboundStream> openAsync(EventLoopGroup eventLoopGroup,
                                                          URI uri, 
                                                          Optional<String> lastEventId, 
                                                          Consumer<ByteBuffer[]> dataConsumer,
                                                          Consumer<Void> closeConsumer, 
                                                          Consumer<Throwable> errorConsumer, 
                                                          Optional<Duration> connectTimeout, 
                                                          Optional<Duration> socketTimeout) {
            
            CompletableFuture<InboundStream> promise = new CompletableFuture<>();
            
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
                         .handler(new InboundHttpChannelInitializer(sslCtx, dataConsumer, closeConsumer, errorConsumer));
                
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
                            writeRequestHeaderAsync(future.channel(), uri, host, port, lastEventId)
                                             .whenComplete((Void, error) -> { if (error == null) {
                                                                                     promise.complete(new NettyHttp11InboundStream(future.channel())); 
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
        
        
        private static CompletableFuture<Void> writeRequestHeaderAsync(Channel channel, URI uri, String host, int port, Optional<String> lastEventId) {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            
            DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri.getRawPath());
            request.headers().set(HttpHeaders.Names.HOST, host + ":" + port);
            request.headers().set(HttpHeaders.Names.USER_AGENT, "sseclient/1.0");
            request.headers().set(HttpHeaders.Names.ACCEPT, "text/event-stream");
            request.headers().set(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
            lastEventId.ifPresent(eventId -> request.headers().set("Last-Event-ID", eventId));
            
            
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
        
        
        
        private NettyHttp11InboundStream(Channel channel) {
            this.channel = channel;
        }

        
        @Override
        public boolean isSuspended() {
            return isSuspended.get();
        }
        
        @Override
        public void suspend() {
            isSuspended.set(true);
            channel.config().setAutoRead(false);
        }
        
        @Override
        public void resume() {
            isSuspended.set(false);
            channel.config().setAutoRead(true);
        }
        
        @Override
        public void close() {
            channel.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            channel.close();
        }
    
        public void terminate() {
            channel.close();
        }
        
        
        private static class InboundHttpChannelInitializer extends ChannelInitializer<SocketChannel> {
            private final SslContext sslCtx;
            private final Consumer<ByteBuffer[]> dataConsumer;
            private final Consumer<Void> closeConsumer; 
            private final Consumer<Throwable> errorConsumer;

            
            public InboundHttpChannelInitializer(SslContext sslCtx, Consumer<ByteBuffer[]> dataConsumer, Consumer<Void> closeConsumer, Consumer<Throwable> errorConsumer) {
                this.sslCtx = sslCtx;
                this.dataConsumer = dataConsumer;
                this.closeConsumer = closeConsumer;
                this.errorConsumer = errorConsumer;
            }
            
            @Override
            public void initChannel(SocketChannel ch) {
                 ChannelPipeline p = ch.pipeline();
                 
                 if (sslCtx != null) {
                     p.addLast(sslCtx.newHandler(ch.alloc()));
                 }
    
                 p.addLast(new HttpClientCodec());
      
                 p.addLast(new HttpContentDecompressor());
                 p.addLast(new HttpInboundHandler(dataConsumer, closeConsumer, errorConsumer));
            }
            
            
        }
    
        
        private static class HttpInboundHandler extends SimpleChannelInboundHandler<HttpObject> {
            private final Consumer<ByteBuffer[]> dataConsumer;
            private final Consumer<Void> closeConsumer;
            private final Consumer<Throwable> errorConsumer;
            
            public HttpInboundHandler(Consumer<ByteBuffer[]> dataConsumer, Consumer<Void> closeConsumer, Consumer<Throwable> errorConsumer) {
                this.dataConsumer = dataConsumer;
                this.closeConsumer = closeConsumer;
                this.errorConsumer = errorConsumer;
            }
            
            
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
                if (msg instanceof HttpResponse) {
                    HttpResponse response = (HttpResponse) msg;
                    
                    if (!response.headers().get("Content-Type").equalsIgnoreCase("text/event-stream")) {
                        errorConsumer.accept(new IOException("got " + response.headers().get("Content-Type") + " data"));
                    }
                    
                } else  if (msg instanceof HttpContent) {
                    HttpContent content = (HttpContent) msg;
                    
                    ByteBuffer[] buffers = content.content().nioBuffers();
                    dataConsumer.accept(buffers);
                    if (content instanceof LastHttpContent) {
                        ctx.close();
                        closeConsumer.accept(null);
                    }
                }
            }
            
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                errorConsumer.accept(cause);
                ctx.close();
                closeConsumer.accept(null);
            }
            
        }
    }
    
    
    
    private class NettyHttp11OutboundStream implements OutboundStream  {
        private final Channel channel;
        private final Consumer<Void> closeConsumer;
        private final AtomicBoolean isOpen = new AtomicBoolean(true);
        
        public NettyHttp11OutboundStream(URI uri, Consumer<Void> closeConsumer) {
            
            try {
                this.closeConsumer = closeConsumer;
                    
                String scheme = (uri.getScheme() == null) ? "http" : uri.getScheme();
                String host =uri.getHost();
                int port = uri.getPort();
                if (port == -1) {
                    if ("http".equalsIgnoreCase(scheme)) {
                        port = 80;
                    } else if ("https".equalsIgnoreCase(scheme)) {
                        port = 443;
                    }
                }
                
                
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
                         .handler(new OutboundHttpChannelInitializer(sslCtx));
                channel = bootstrap.connect(host, port).sync().channel();
    
                
                // write request header
                DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri.getRawPath());
                request.headers().set(HttpHeaders.Names.HOST, host + ":" + port);
                request.headers().set(HttpHeaders.Names.USER_AGENT, "sseclient/1.0");
                request.headers().set(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
                channel.writeAndFlush(request);
                
            } catch (InterruptedException | SSLException e) {
                throw new RuntimeException(e);
            }
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
            if (isOpen.getAndSet(false)) {
                channel.close();
                closeConsumer.accept(null);
            }
        }
        
        private class OutboundHttpChannelInitializer extends ChannelInitializer<SocketChannel> {
        
            private final SslContext sslCtx;
            
            public OutboundHttpChannelInitializer(SslContext sslCtx) {
                this.sslCtx = sslCtx;
            }
            
            @Override
            public void initChannel(SocketChannel ch) {
                 ChannelPipeline p = ch.pipeline();
                 
                 if (sslCtx != null) {
                     p.addLast(sslCtx.newHandler(ch.alloc()));
                 }
    
                 p.addLast(new HttpClientCodec());
      
                 p.addLast(new HttpContentDecompressor());
                 p.addLast(new HttpInboundHandler());
            }
        }
    
        
        private class HttpInboundHandler extends SimpleChannelInboundHandler<HttpObject> {
            
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
                if (msg instanceof HttpResponse) {
                    HttpResponse response = (HttpResponse) msg;
                    
                } else  if (msg instanceof HttpContent) {
                    HttpContent content = (HttpContent) msg;
                    
                    if (content instanceof LastHttpContent) {
                        ctx.close();
                        terminate();
                    }
                }
            }
            
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                // TODO Auto-generated method stub
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
        public OutboundStream newOutboundStream(URI uri, Consumer<Void> closeConsumer) {
            return delegate.newOutboundStream(uri, closeConsumer);
        }
        

        @Override
        public CompletableFuture<InboundStream> openInboundStreamAsync(URI uri,
                                                                       Optional<String> lastEventId,
                                                                       Consumer<ByteBuffer[]> dataConsumer,
                                                                       Consumer<Void> closeConsumer,
                                                                       Consumer<Throwable> errorConsumer,  
                                                                       Optional<Duration> connectTimeout,
                                                                       Optional<Duration> socketTimeout) {
            return delegate.openInboundStreamAsync(uri, 
                                                   lastEventId,
                                                   dataConsumer, 
                                                   closeConsumer,
                                                   errorConsumer, 
                                                   connectTimeout, 
                                                   socketTimeout);
        }
        
        @Override
        public void close() {

        }
    }
}        
