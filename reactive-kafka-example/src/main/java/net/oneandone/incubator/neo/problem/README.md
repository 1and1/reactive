Problem reporting support
=========================

The problem reporting support implements reporing problems for HTTP APIs according to [Problem Details for HTTP APIs](https://tools.ietf.org/html/draft-ietf-appsawg-http-problem-02)


Server-side usage
--------------




**Example 1 - mapping a non http problem-aware exception (such as NumberFormatException) by mapping**
```
@GET
@Path("/topics/{topicname}")
@Produces("application/vnd.ui.example.topic+json; qs=0.8")
public TopicRepresentation getTopicJson(final @Context UriInfo uriInfo, 
							            final @PathParam("topicname") String topicname) {
     
     long id = Long.paseLong(topicname); // may throw a NumberFormatException   
	
     //...    
}
```


**Example 2 - using a problem exceptions** 
```
@GET
@Path("/topics/{topicname}")
@Produces("application/vnd.ui.example.topic+json; qs=0.8")
public TopicRepresentation getTopicJson(final @Context UriInfo uriInfo, 
							            final @PathParam("topicname") String topicname) {
        
     if (topicname.startsWith("@")) {
         // <<ToDo update this example>>  here a problem exception should be thrown
		 //...
     }    

     // ...
}
```


**Example 3 - generating a http response in a direct way**
```
@GET
@Path("/topics/{topicname}")
@Produces("application/vnd.ui.example.topic+json; qs=0.8")
public TopicRepresentation getTopicJson(final @Context UriInfo uriInfo, 
							            final @PathParam("topicname") String topicname) {
     
     if (topicname.startsWith("@")) {
         // <<ToDo update this example>> here a problem error response should be returned
		 // ...
     }   

     // ...     
}
```



Client-side usage
--------------


**Example 1 - response code oriented way**
```
Response resp = client.target(uri + "/rest/topics/" + topicName + "/454")
                      .request()
                      .post(myEntity);

if ((resp.getStatus() / 100) == 2) {
    final DeliveryInfo delivery = resp.readEntity(DeliveryInfo.class);
    //...

} else if ((resp.getStatus() / 100) == 4) {
    // <<ToDo update this example>> identifying and handling dedicated problem types   

} else {
   //...
}

```


**Example 2 - try-catch oriented way**
```
try {
   final DeliveryInfo delivery = client.target(uri + "/rest/topics/" + topicName + "/454")
                                       .request()
                                       .post(myEntity, DeliveryInfo.class);

   //...
} catch (BadRequestException badRequestError) {
    // <<ToDo update this example>> identifying and handling dedicated problem types

} catch (ClientErrorException genericClientError) {
    // <<ToDo update this example>> identifying and handling dedicated problem types
            
} catch (RuntimeException rt) {
    //...
}

```


Internal implementation notes
--------------------

The problem reporting should support 

1. a **library-oriented approach** where library specific exceptions will be mapped to HTTP problem responses. The library stays independent of the remoting layer.
 
2. smaller **applications where no layering is used**, which means the core exception have not to be indepndent of the remoting layer         

3. user specific problem types as well as generic problemtypes defined by app-indepentend libraries. By supporting generic problemtypes the artefacts of the Problem reporting lib should be extenable in a smooth way

4. a fluent interface style. 

5. prefer immutable artifacts.    
