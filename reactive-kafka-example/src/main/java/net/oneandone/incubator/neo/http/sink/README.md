HttpSink
========

The HttpSink is a client lib to perform ***one-way*** styled communication in a ***resilient*** way. Even though the HTTP protocol is a *request-response* protocol it is sometime used to implemented *logical* one-way communication on top of HTTP such as pushing event messages to the resource server. For such a communication the HTTP server response represents the acknowledge/disacknowledge response of the message delivery.
 
This library implements default behavior to handle the acknowledge/disacknowledge response. In case of disacknowledge this lib supports retrying the call in a delayed style. To support high resiliency the lib also allows buffering pending send jobs on the local disk. 

In the example below a HttpSink instance is used to send a json-based event to a dedicated Http endpoint. Internally the HttpSink uses a [JAX-RS client](https://docs.oracle.com/javaee/7/api/javax/ws/rs/client/package-summary.html) instance to perfom the query. 


```
// open a new sink
HttpSink sink = HttpSink.target(myUri)
                        .open();



// send some events

sink.submit(new CustomerChangedEvent(id)                                // the bean 
            "application/vnd.example.event.customerdatachanged+json");  // the mime type 

//..        
sink.submit(new CustomerChangedEvent(anotherId), 
            "application/vnd.example.event.customerdatachanged+json");



// close the sink
sink.close();
```

By default the `POST` method is used to perform the request. To use another mothod the `withMethod(...)` has to be used   

```
HttpSink sink = HttpSink.target(myUri)
                        .withMethod(Method.PUT)
                        .open();
// ...


sink.submit(new CustomerChangedEvent(id), 
            "application/vnd.example.event.customerdatachanged+json");

//..        
```

Event though some dedicated HTTP methods such as `PUT` are idemepotent the HttpSink does not retry the call by default. To define the rety sequence the method `withRetryAfter(...)` is used. By calling this methods the duration between reties will be defined. In the example below at maximum 3 retries will be performed which means in total at maximum 4 calls occur.   

```
HttpSink sink = HttpSink.target(myUri)
                        .withMethod(Method.PUT)
						.withRetryAfter(Duration.ofSeconds(2), Duration.ofMillis(30), Duration.ofMinutes(5))
                        .open();

// ...
sink.submit(new CustomerChangedEvent(id),
            "application/vnd.example.event.customerdatachanged+json");


//...
```

By default the message send job will be buffered in-memory. All these jobs will be lost, if the sink is closed or the service crashes. To keep the messages send jobs persistent on disk to survive closing the sink or a service crash the directory has to be set by using the `withPersistency(...)` method. In this case first the submission will be persists to disc before performing any other action. Please consider that persisting json messages is support only, currently. 

```
HttpSink sink = HttpSink.target(myUri)
                        .withRetryAfter(Duration.ofSeconds(2), Duration.ofMillis(30), Duration.ofMinutes(5))
					    .withPersistency(mySubmissionDir)
                        .open();

// ...
sink.submit(new CustomerChangedEvent(id),
            "application/vnd.example.event.customerdatachanged+json");


//...
```

To limit the max queue size of the pending send job treject the max buffer size can be reduced by performing `withPersistency(...)` (default is unlimited). In this case the `submit` method will be rejected, if the max buffer size is execeeded.    

```
HttpSink sink = HttpSink.target(myUri)
                        .withRetryAfter(Duration.ofSeconds(2), Duration.ofMillis(30), Duration.ofMinutes(5))
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
                        .withRetryAfter(Duration.ofSeconds(2), Duration.ofMillis(30), Duration.ofMinutes(5))
					    .withPersistency(myWorkingDir)
                        .withRetryBufferSize(10000)  //max 10000 pending
                        .withRejectOnStatus(400, 403, 405, 406, 408, 409, 410, 413, 414, 415, 416, 417) jobs
                        .open();

// ...
sink.submit(new CustomerChangedEvent(id), 
            "application/vnd.example.event.customerdatachanged+json");


//...
```

By calling the `submit` method first it will be tried to perform the query. The submit methods returns either the call is sucessfully or it is failed for the *first* time. By calling the `submitAsync` the call returns immediately without waiting for the response. In this case a `CompletableFuture` is returned   

```
HttpSink sink = HttpSink.target(myUri)
                        .withRetryAfter(Duration.ofSeconds(2), Duration.ofMillis(30), Duration.ofMinutes(5))
					    .withPersistency(myWorkingDir)
                        .open();
// ...


CompletableFuture<Submission> submission = sink.submitAsync(new CustomerChangedEvent(id), 
                                                            "application/vnd.example.event.customerdatachanged+json");


//...
```