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
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLException;

import net.oneandone.reactive.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;





class NettyBasedChannelProvider implements StreamProvider {
    private static final Logger LOG = LoggerFactory.getLogger(NettyBasedChannelProvider.class);
    
    private static final ImmutableSet<Integer> REDIRECT_STATUS_CODES = ImmutableSet.of(301, 302, 307);
    private static final ImmutableSet<Integer> GET_REDIRECT_STATUS_CODES = ImmutableSet.of(301, 302, 303, 307);
    
    private final EventLoopGroup eventLoopGroup;
    
    
    
    
    NettyBasedChannelProvider() {
        eventLoopGroup = new NioEventLoopGroup();
    }
    
    
    @Override
    public CompletableFuture<Void> closeAsync() {
        NettyFutureListenerPromiseAdapter<Object> promise = new NettyFutureListenerPromiseAdapter<>();
        eventLoopGroup.shutdownGracefully().addListener(promise);        
        return promise.thenApply(obj -> null);
    }
    
    
    
    

    @Override
    public CompletableFuture<Stream> newStreamAsync(ConnectionParams params) {
        CompletableFuture<Stream> connectPromise = new CompletableFuture<>();
        openStreamAsync(params, connectPromise);
        return connectPromise;
    }
    
 
    private void openStreamAsync(ConnectionParams params, CompletableFuture<Stream> connectPromise) {
        openStreamAsync(params, new StatefulHttpChannelHandler(params, connectPromise));
    }
 
    
    private void openStreamAsync(ConnectionParams params, HttpChannelHandler channelHandler) {
        try {
            SslContext sslCtx= ("https".equalsIgnoreCase(URIUtils.getScheme(params.getUri()))) ? SslContext.newClientContext() : null;
            
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                     .channel(NioSocketChannel.class)
                     .handler(new HttpChannelInitializer(sslCtx, channelHandler));
            
            if (params.getConnectTimeout().isPresent()) {
                bootstrap = bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) params.getConnectTimeout().get().toMillis());
            }
    
            
            
            
            LOG.debug("[" + params.getId() + "] - opening channel with "  + bootstrap.toString());
            
            ChannelFutureListener listener = new ChannelFutureListener() {
    
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        channelHandler.onConnect(future.channel());
                    } else {
                        channelHandler.onError(future.channel(), future.cause());
                    }
                }
            };
            bootstrap.connect(params.getUri().getHost(), URIUtils.getPort(params.getUri())).addListener(listener);
            
            
        } catch (SSLException | RuntimeException e) {
            channelHandler.onError(null, new ConnectException("could not connect to " + params.getUri(), e));
        }
    }

    
    


    
    private static interface HttpChannelHandler  {
        
        void onConnect(Channel channel);
        
        void onResponseHeader(Channel channel, HttpResponse response);
        
        void onClosed(Channel channel);
        
        void onError(Channel channel, Throwable error);
        
        void onData(Channel channel, ByteBuffer[] data);
    }
    


    
    private static interface StateContext {
        
        void setState(HttpChannelHandler state);
    }
    


    
    
    private class StatefulHttpChannelHandler implements HttpChannelHandler, StateContext {
        private final AtomicReference<HttpChannelHandler> stateRef;
        
       
        public StatefulHttpChannelHandler(ConnectionParams params, CompletableFuture<Stream> connectPromise) {
            this.stateRef = new AtomicReference<>(new ConnectStateHandler(this, params, connectPromise));
        }
        
        public void setState(HttpChannelHandler state) {
            stateRef.set(state); 
        }
        
        @Override
        public void onConnect(Channel channel) {
            stateRef.get().onConnect(channel);
        }
        
        @Override
        public void onResponseHeader(Channel channel, HttpResponse response) {
            stateRef.get().onResponseHeader(channel, response);
        }
        
        @Override
        public void onData(Channel channel, ByteBuffer[] data) {
            stateRef.get().onData(channel, data);
        }
        
        @Override
        public void onClosed(Channel channel) {
            stateRef.get().onClosed(channel);
        }
        
        @Override
        public void onError(Channel channel, Throwable error) {
            stateRef.get().onError(channel, error);
        }   
    }
    
    
    

    private static abstract class HandlerState implements HttpChannelHandler, StateContext {
        private final ConnectionParams params;
        private final StateContext context;

        
        public HandlerState(ConnectionParams params, StateContext context) {
            this.params = params;
            this.context = context;
        }
        
        public void onConnect(Channel channel) {
            onError(channel, new IllegalStateException("got connect notify"));
        }
        
        public void onResponseHeader(Channel channel, HttpResponse response) {
            onError(channel, new IllegalStateException("got unexpected response header"));
        }
        
        public void onClosed(Channel channel) {
            onError(channel, new ClosedChannelException());
        }
        
        public void onError(Channel channel, Throwable error) {
            log(channel, "error occured " + error.toString());
        }
        
        public void onData(Channel channel, ByteBuffer[] data) {
            
        }
        
        protected ConnectionParams getParams() {
            return params;
        }

        @Override
        public void setState(HttpChannelHandler state) {
            context.setState(state);
        }

        protected void log(Channel channel, String msg) {
            LOG.debug("[" + getId(channel) + "] " + msg);            
        }
        
        protected String getId(Channel channel) {
            String channelId = (channel == null) ? "-1" : Integer.toString(Math.abs(channel.hashCode()));
            return params.getId() + "#" + channelId;
        }
        
        protected void setNullState() {
            context.setState(new NullState());
        }
    }


        
    private static class NullState implements HttpChannelHandler {
        
        @Override
        public void onConnect(Channel channel) { }
        
        @Override
        public void onResponseHeader(Channel channel, HttpResponse response) { }
        
        @Override
        public void onData(Channel channel, ByteBuffer[] data) {  }
        
        @Override
        public void onClosed(Channel channel) {  }
        
        @Override
        public void onError(Channel channel, Throwable error) {  }
    }
    
    

    
    private class ConnectStateHandler extends HandlerState {
        private final CompletableFuture<Stream> connectPromise;
        
        public ConnectStateHandler(StateContext context, ConnectionParams params, CompletableFuture<Stream> connectPromise) {
            super(params, context);
            this.connectPromise = connectPromise;
        }

        
        @Override
        public void onConnect(Channel channel) {
            log(channel, " opened. Sending " + getParams().getMethod() + " request header " + getParams().getUri().getRawPath() + ((getParams().getUri().getRawQuery() == null) ? "" : "?" + getParams().getUri().getRawQuery()));
            
            DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.valueOf(getParams().getMethod()), getParams().getUri().getRawPath() + ((getParams().getUri().getRawQuery() == null) ? "" : "?" + getParams().getUri().getRawQuery()));
            request.headers().set(HttpHeaders.Names.HOST, getParams().getUri().getHost()+ ":" + URIUtils.getPort(getParams().getUri()));
            request.headers().set(HttpHeaders.Names.USER_AGENT, "sseclient/1.0");
            getParams().getHeaders().forEach((name, value) -> request.headers().set(name, value));
            
            setState(new ResponseMessageStateHandler(this, getParams(), connectPromise));
            channel.writeAndFlush(request);
        }
        
                
        @Override
        public void onError(Channel channel, Throwable error) {
            super.onError(channel, error);
            setNullState();
            connectPromise.completeExceptionally(error);
        }
    }
    
    
    
    
    private class ResponseMessageStateHandler extends HandlerState {
        private final CompletableFuture<Stream> connectPromise;

        
        ResponseMessageStateHandler(StateContext context, ConnectionParams params, CompletableFuture<Stream> connectPromise) {
            super(params, context);
            this.connectPromise = connectPromise;
        }
        
        
        @Override
        public void onResponseHeader(Channel channel, HttpResponse response) {
            
            int status = response.getStatus().code();
            String locationURI = response.headers().get("location");
            
            // success
            if ((status / 100) == 2) {
                log(channel, " got " + status + " response. start stream handling");
                
                Http11Stream stream = new Http11Stream(getId(channel), channel);
                setState(new  DataStateHandler(this, getParams(), stream));
                connectPromise.complete(stream);

            // no success
            } else {
                setNullState();
                channel.close();

                // redirect
                if ((locationURI != null) && isRedirectSupported(getParams().getMethod(), response) && (getParams().getNumFollowRedirects() > 0)) {
                    int newNumFollowRedirects = getParams().getNumFollowRedirects() - 1;
                    log(channel, "follow redirect " + locationURI + " (remaining redirect trials: " + newNumFollowRedirects+ ")");
                    
                    openStreamAsync(new ConnectionParams(getParams().getId(), 
                                                         URI.create(locationURI), 
                                                         getParams().getMethod(), 
                                                         getParams().getHeaders(), 
                                                         getParams().isFailOnConnectError(), 
                                                         newNumFollowRedirects, 
                                                         getParams().getDataHandler(), 
                                                         getParams().getConnectTimeout()), connectPromise);
                // error 
                } else {                
                    onError(channel, new IOException("got unexpected " + status + " response"));
                }
            }
        }
        
        
        @Override
        public void onError(Channel channel, Throwable error) {
            super.onError(channel, error);
            setNullState();
            connectPromise.completeExceptionally(error);
        }
        
        

        private boolean isRedirectSupported(String method, HttpResponse response) {
            return REDIRECT_STATUS_CODES.contains(response.getStatus().code()) ||
                   (method.equalsIgnoreCase("GET") && GET_REDIRECT_STATUS_CODES.contains(response.getStatus().code()));
        }
    }
    
    
    
    private static class DataStateHandler extends HandlerState {
        private final Http11Stream stream;
        
        DataStateHandler(StateContext context, ConnectionParams params, Http11Stream stream) {
            super(params, context);
            this.stream = stream;
        }

        @Override
        public void onData(Channel channel, ByteBuffer[] data) {
            getParams().getDataHandler().onContent(getId(channel), data);
        }

        @Override
        public void onError(Channel channel, Throwable error) {
            super.onError(channel, error);
            setNullState();
            getParams().getDataHandler().onError(getId(channel), error);
        }
        
        @Override
        public void onClosed(Channel channel) {
            stream.close();
            getParams().getDataHandler().onError(getId(channel), new ClosedChannelException());
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
        public String getId() {
            return id;
        }
        
        @Override
        public CompletableFuture<Void> writeAsync(String msg) {
            NettyFutureListenerPromiseAdapter<Void> promise = new NettyFutureListenerPromiseAdapter<>();
        
            synchronized (channel) {
                if (channel.isWritable()) {
                    channel.writeAndFlush(new DefaultHttpContent(Unpooled.copiedBuffer(msg, StandardCharsets.UTF_8))).addListener(promise);
                } else {
                    promise.completeExceptionally(new IllegalStateException("channel is not writeable (no space left in socket send buffer?)"));
                }
            }
            
            return promise;
        }
        
        
        @Override
        public boolean isReadSuspended() {
            return !channel.config().isAutoRead();
        }
        
        @Override
        public void suspendRead() {
            if (channel.config().isAutoRead()) {
                LOG.debug("[" + id + "] suspended");
                channel.config().setAutoRead(false);
            }
        }
        
        @Override
        public void resumeRead() {
            if (!channel.config().isAutoRead()) {
                LOG.debug("[" + id + "] resumed");
                channel.config().setAutoRead(true);
            }
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
    
    
    
    
    
    
    
    
    
    private static class HttpChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final SslContext sslCtx;
        private final HttpChannelHandler channelHandler;

        public HttpChannelInitializer(SslContext sslCtx, HttpChannelHandler channelHandler) {
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
             p.addLast(new HttpInboundHandler());
        }
        

        
        private class HttpInboundHandler extends SimpleChannelInboundHandler<HttpObject> {
            
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
                
                if (msg instanceof HttpResponse) {
                    channelHandler.onResponseHeader(ctx.channel(), (HttpResponse) msg);
                    
                } else  if (msg instanceof HttpContent) {
                    HttpContent content = (HttpContent) msg;
                    channelHandler.onData(ctx.channel(), content.content().nioBuffers());
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
                channelHandler.onClosed(ctx.channel());
            }
            
            private void notifyError(ChannelHandlerContext ctx, Throwable error) {
                channelHandler.onError(ctx.channel(), error);
                ctx.close();
            }   
        }
    }
}        