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
package net.oneandone.reactive.rest;




import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import net.oneandone.reactive.rest.container.ResultConsumer;



  
@Path("/MyResource")
public class MyResource {
    
    private final Dao dao = new Dao();
    
 
  
    @Path("/{id}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void retrieveAsync(@PathParam("id") long id, @Suspended AsyncResponse resp) {
        
        if (id == 999)  {

            new Thread() {
                
                @Override
                public void run() {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ignore) { }
                    
                    resp.resume(new NotFoundException("not found"));
                }
                
            }.start();
        }
        
        dao.readAsync(id)
           .whenComplete(ResultConsumer.writeTo(resp));
    }
    
    
    @Path("/{id}")
    @DELETE
    public void deleteAsync(@PathParam("id") long id, @Suspended AsyncResponse resp) {
        dao.deleteAsync(id)
           .whenComplete(ResultConsumer.writeTo(resp));
    }
}