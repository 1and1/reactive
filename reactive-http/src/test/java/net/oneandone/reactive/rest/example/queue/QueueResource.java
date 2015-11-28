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
package net.oneandone.reactive.rest.example.queue;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.reactivestreams.Publisher;

import net.oneandone.reactive.ReactiveSink;
import net.oneandone.reactive.pipe.Pipes;
import net.oneandone.reactive.rest.example.queue.QueueManager.Message;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.servlet.ServletSseSubscriber;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;






@Path("/queues")
public class QueueResource implements Closeable { 
    
    private QueueManager<String> queueManager = null;

    
    private QueueManager<String> getQueueManager() {
        synchronized (this) {
            if (queueManager == null) {
                queueManager = new ActiveMQQueueManager(new File("mqqueue" + File.separator + UUID.randomUUID().toString()));
            }
            
            return queueManager;
        }
    }
    

    @Override
    public void close() throws IOException {
        if (queueManager != null) {
            queueManager.close();
        }
    }

    
    @POST
    @Path("/{queuename}/messages")
    public Response publishMessage(@PathParam("queuename") String queuename, @QueryParam("ttl") Integer ttlSec, InputStream bodyStream) throws IOException {
        String uuid = "rg" + UUID.randomUUID().toString();
        publishMessage(queuename, uuid, ttlSec, bodyStream);
        return Response.ok().header("x-ui-message-id", uuid).build();
    }

    @PUT
    @Path("/{queuename}/messages/{uuid}")
    public void publishMessage(@PathParam("queuename") String queuename, @PathParam("uuid") String uuid, @QueryParam("ttl") Integer ttlSec, InputStream bodyStream) throws IOException {
        Message<String> message = new Message<String>(uuid, new String(ByteStreams.toByteArray(bodyStream), Charsets.UTF_8), (ttlSec == null) ? null : Duration.ofSeconds(ttlSec));
        
        ReactiveSink<Message<String>> sink = ReactiveSink.publish(getQueueManager().newSubscriber(queuename));
        sink.write(message);
    }
    
    
    
    @GET
    @Path("/{queuename}/messages")
    @Produces("text/event-stream")
    public void consumeMessageSse(@Context HttpServletRequest request, @Context HttpServletResponse response, @PathParam("queuename") String queuename) throws IOException {
        Publisher<Message<String>> publisher = getQueueManager().newPublisher(queuename);
        
        Pipes.from(publisher)
             .map(message -> ServerSentEvent.newEvent().id(message.getId()).data(message.getData()))
             .to(new ServletSseSubscriber(request, response));
    }
    
    
    

    @GET
    @Path("/{queuename}/messages")
    @Produces("text/plain")
    public void consumeMessagePlain(@PathParam("queuename") String queuename) throws IOException {
        
    }
}