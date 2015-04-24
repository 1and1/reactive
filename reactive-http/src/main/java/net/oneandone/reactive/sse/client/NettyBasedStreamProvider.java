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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javax.net.ssl.SSLException;



class NettyBasedStreamProvider implements StreamProvider {
    
    private final EventLoopGroup eventLoopGroup;
    
    public NettyBasedStreamProvider() {
        eventLoopGroup = new NioEventLoopGroup();
    }
    
    @Override
    public void close() {
        eventLoopGroup.shutdownGracefully();
    }
    
    
    @Override
    public InboundStream newInboundStream(URI uri, Consumer<ByteBuffer[]> dataConsumer, Consumer<Throwable> errorConsumer) {
        return new NettyHttp11InboundStream(uri, dataConsumer, errorConsumer);
    }
    
    
    @Override
    public OutboundStream newOutboundStream(URI uri) {
        return new NettyHttp11OutboundStream(uri);
    }

    
    


    class NettyHttp11InboundStream implements InboundStream  {
        private final Channel channel;
        private final Consumer<ByteBuffer[]> dataConsumer;
        private final Consumer<Throwable> errorConsumer;
        private final AtomicBoolean isSuspended = new AtomicBoolean(false); 
        
        public NettyHttp11InboundStream(URI uri, Consumer<ByteBuffer[]> dataConsumer, Consumer<Throwable> errorConsumer) {
            try {
                this.dataConsumer = dataConsumer;
                this.errorConsumer = errorConsumer;
                
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
                         .handler(new HttpChannelInitializer(sslCtx));
                channel = bootstrap.connect(host, port).sync().channel();
    
                
                // write request header
                DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri.getRawPath());
                request.headers().set(HttpHeaders.Names.HOST, host + ":" + port);
                request.headers().set(HttpHeaders.Names.USER_AGENT, "sseclient/1.0");
                request.headers().set(HttpHeaders.Names.TRANSFER_ENCODING, HttpHeaders.Values.CHUNKED);
                
                channel.writeAndFlush(request);
                
                
            } catch (InterruptedException | SSLException e) {
                throw new RuntimeException(e);
            }
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
        
        
        private class HttpChannelInitializer extends ChannelInitializer<SocketChannel> {
        
            private final SslContext sslCtx;
            
            public HttpChannelInitializer(SslContext sslCtx) {
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
                    
                    if (!response.headers().get("Content-Type").equalsIgnoreCase("text/event-stream")) {
                        errorConsumer.accept(new IOException("got " + response.headers().get("Content-Type") + " data"));
                    }
                    
                } else  if (msg instanceof HttpContent) {
                    HttpContent content = (HttpContent) msg;
                    
                    ByteBuffer[] buffers = content.content().nioBuffers();
                    dataConsumer.accept(buffers);
                    if (content instanceof LastHttpContent) {
                        ctx.close();
                    }
                }
            }
            
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                errorConsumer.accept(cause);
            }
        }
    }
    
    
    
    class NettyHttp11OutboundStream implements OutboundStream  {
        private final Channel channel;
        
        public NettyHttp11OutboundStream(URI uri) {
            try {
                
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
                         .handler(new HttpChannelInitializer(sslCtx));
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
            channel.close();
        }
    
        public void terminate() {
            channel.close();
        }
        
        private class HttpChannelInitializer extends ChannelInitializer<SocketChannel> {
        
            private final SslContext sslCtx;
            
            public HttpChannelInitializer(SslContext sslCtx) {
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
}        
