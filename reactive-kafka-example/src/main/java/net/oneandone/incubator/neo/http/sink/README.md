HttpSink
========

The HttpSink is a client lib to perform ***one-way*** styled communication in a ***resilient*** way. Even though the HTTP protocol is a *request-response* protocol it is sometime used to implemented *logical* one-way communication on top of HTTP such as pushing event messages to the resource server. For such a communication the HTTP server response represents the acknowledge/disacknowledge response of the message delivery.
 
This library implements default behavior to handle the acknowledge/disacknowledge response. In case of disacknowledge this lib supports retrying the call in a delayed style. To support high resiliency the lib also allows buffering pending submission on the local disk. 

In the example below a HttpSink instance is used to send a json-based message to a dedicated Http endpoint. Internally the HttpSink uses a [JAX-RS client](https://docs.oracle.com/javaee/7/api/javax/ws/rs/client/package-summary.html) instance to perfom the query. 


```
// open a new sink
HttpSink sink = HttpSink.target(myUri)
                        .open();



// send some events

sink.submit(new CustomerChangedEvent(id)                                // the bean 
            "application/vnd.example.event.customerdatachanged+json");  // the mime type as string

//..        
sink.submit(new AddressChangedEvent(anotherId),                        // the bean
            MediaType.APPLICATION_JSON);                               // the mime type as object



// close the sink
sink.close();
```

By default the `POST` method is used to perform the request. To use another method the `withMethod(...)` has to be used   

```
HttpSink sink = HttpSink.target(myUri)
                        .withMethod(Method.PUT)
                        .open();

// ...
sink.submit(new CustomerChangedEvent(id), 
            "application/vnd.example.event.customerdatachanged+json");

//..        
```

Event though some dedicated HTTP methods such as `PUT` are idemepotent the HttpSink does not retry the call by default. To define the rety sequence the method `withRetryAfter(...)` is used. By calling this methods the duration between reties will be defined. In the example below at maximum 3 retries will be performed which means in total at maximum 4 calls occur. If all 4 calls fails, the submission will be discarded.  

```
HttpSink sink = HttpSink.target(myUri)
                        .withMethod(Method.PUT)
						.withRetryAfter(Duration.ofSeconds(2), Duration.ofSeconds(30), Duration.ofMinutes(5))
                        .open();

// ...
sink.submit(new CustomerChangedEvent(id),
            "application/vnd.example.event.customerdatachanged+json");


//...
```

By default the submissions will be buffered in-memory. All these submissions will be lost, if the sink is closed or the service crashes. To keep the submissions persistent on disk to survive closing the sink or a service crashes the store directory has to be set by using the `withPersistency(...)` method. In this case first the submission will be persists to disc before performing any other action. Please consider that persisting json messages is support only, currently. 

```
HttpSink sink = HttpSink.target(myUri)
                        .withRetryAfter(Duration.ofSeconds(2), Duration.ofSeconds(30), Duration.ofMinutes(5))
					    .withPersistency(mySubmissionDir)
                        .open();

// ...
sink.submit(new CustomerChangedEvent(id),
            "application/vnd.example.event.customerdatachanged+json");


//...
```

The  `withPersistency(...)` accepts a boolean value also. If true, the user home dir will be used to store the pending submission to the disc,  
```
HttpSink sink = HttpSink.target(myUri)
                        .withRetryAfter(Duration.ofSeconds(2), Duration.ofSeconds(30), Duration.ofMinutes(5))
                        .withPersistency(true)
                        .open();

// ...
sink.submit(new CustomerChangedEvent(id),
            "application/vnd.example.event.customerdatachanged+json");


//...
```




To limit the max queue size of the pending submissions the max buffer size can be reduced by performing `withRetryBufferSize(...)` (default is unlimited). In this case the `submit` method will be rejected, if the max buffer size is execeeded.    

```
HttpSink sink = HttpSink.target(myUri)
                        .withRetryAfter(Duration.ofSeconds(2), Duration.ofSeconds(30), Duration.ofMinutes(5))
					    .withPersistency(myWorkingDir)
                        .withRetryBufferSize(10000)  //max 10000 pending jobs
                        .open();

// ...
sink.submit(new CustomerChangedEvent(id), 
            "application/vnd.example.event.customerdatachanged+json");


//...
```

Furthermore the `submit` method will also be rejected, if some dedicated error codes is returned by the server. For instance, if the server returns a `400` or a '410', the submission will be rejected by default. This behavior can be overwritten by using the method `withRejectOnStatus(…)`. Hoewever, in general the default reject status list should be OK for you. 

```
HttpSink sink = HttpSink.target(myUri)
                        .withRetryAfter(Duration.ofSeconds(2), Duration.ofSeconds(30), Duration.ofMinutes(5))
					    .withPersistency(myWorkingDir)
                        .withRetryBufferSize(10000)  //max 10000 pending
                        .withRejectOnStatus(400, 403, 405, 406, 408, 409, 410, 413, 414, 415, 416, 417)  
                        .open();

// ...
sink.submit(new CustomerChangedEvent(id), 
            "application/vnd.example.event.customerdatachanged+json");


//...
```

By calling the `submit` method first it will be tried to perform the submission. The submit methods returns either the call is successfully or it is failed for the *first* time. By calling the `submitAsync` the call returns immediately without waiting for the response. In this case a `CompletableFuture` is returned.

```
HttpSink sink = HttpSink.target(myUri)
                        .withRetryAfter(Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofMinutes(5), Duration.ofMinutes(30), Duration.ofHours(7))
					    .withPersistency(myWorkingDir)
                        .open();
// ...


CompletableFuture<Submission> submission = sink.submitAsync(new CustomerChangedEvent(id), 
                                                            "application/vnd.example.event.customerdatachanged+json");


//...
```

In most cases the `submitAsync` approach is preferred as shown above by setting the retry sequence `withRetryAfter` and a store directory `withPersistency`. This allows you to submit an event message in a resilient way without blocking the main program flow. 


Production-ready example 
----------------------
In the example below the http sink will be opened by instantiating the example service. The  `withRetryAfter(...)`  method is used to define the retry sequence. To store pending submissions a sub directory of the user home dir is used. It is expected that the service will always run in the context of the same user. Furthermore the buffer for pending submissions is limited to 10000. If this size is exceeded, new submissions will be discarded. 

The httpsink is bound on the lifycycle of the example service. If the example service is closed, the httpsink will also be closed. 

To submit messages the example service uses the `submitAsync` method to avoid blocking behavior by submitting the message. The `submitAsync` returns immediately independent if the message is already sent or not. Send errors will be ignored on application level, implicitly. Here, the business rule allows that message will be discarded under some rare error circumstances. However, in cases of temporary errors the chance is very high that the message will be sent successfully. Base on the activated persistency and the retry sequence erroneous submissions will be stored on disc and be retried later.

  
```  
public class MyExampleService implements Closeable {
   private final HttpSink httpSink;
   // ...
	
   public MyExampleService(final URI sinkUri) {
      //...
		
      this.httpSink = HttpSink.target(sinkUri)
                              .withRetryAfter(Duration.ofSeconds(2), Duration.ofSeconds(30), Duration.ofMinutes(5), Duration.ofMinutes(30))
                              .withPersistency(true) 
                              .withRetryBufferSize(10000)    
                              .open();
   }
	
   @Override
   public void close() {
      httpSink.close();
   }
	
   public void myBusinessMethod() {
      // ...      
		
      httpSink.submitAsync(myEvent, MediaType.APPLICATION_JSON); 
   }
}
```  
	