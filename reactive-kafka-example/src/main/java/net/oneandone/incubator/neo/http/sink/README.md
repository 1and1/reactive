HttpSink
========

The HttpSink is a client lib to perform one-way styled communication in a ***resilient*** way. Even though the http protocol is a *request-response* protocol it is sometimes used to implemented *logical* one-way communication such as pushing event messages to the resource server. For such a communication the http server response represents the acknowledge/disacknowledge message.
 
This library implements default behavior to handle the acknowledge/disacknowledge. In case of disacknowledge the lib supports retrying the call in a delayed style. To support high resiliency the lib also allows buffering pending messages on the local disk. 

In the example below a HttpSink instance is used to send a json event to a dedicated Http endpoint. 


```
HttpSink sink = HttpSink.target(myUri)
                        .open();
// ...


sink.submit(new CustomerChangedEvent(id), "application/vnd.example.event.customerdatachanged+json");

//..        
sink.submit(new CustomerChangedEvent(anotherId), "application/vnd.example.event.customerdatachanged+json");



//
sink.close();
```

By default the `POST` method is used to perform the request. To use another mothod the `withMethod(...)` has to be used   

```
HttpSink sink = HttpSink.target(myUri)
                        .withMethod(Method.PUT)
                        .open();
// ...


sink.submit(new CustomerChangedEvent(id), "application/vnd.example.event.customerdatachanged+json");

//..        
```

Event though the dedicated http methods such as `PUT` the HttpSink do not retry the call by default. To set the rety sequence the method `withRetryAfter(...)` is used. By calling this methods the duration between reties is set. In the example below at maximum 3 retries will be performed.    

```
HttpSink sink = HttpSink.target(myUri)
                        .withMethod(Method.PUT)
						.withRetryAfter(Duration.ofSeconds(2), Duration.ofMillis(30), Duration.ofMinutes(5))
                        .open();
// ...


sink.submit(new CustomerChangedEvent(id), "application/vnd.example.event.customerdatachanged+json");


//...
```

By default the message send job will be buffered in-memory. All these jobs will be lost, if the services is restarted. To keep the messages send jobs persistent on disk the directory has to be set by using the `withPersistency(...)` method. In this case failed send jobs will be stored on disk.  

```
HttpSink sink = HttpSink.target(myUri)
                        .withRetryAfter(Duration.ofSeconds(2), Duration.ofMillis(30), Duration.ofMinutes(5))
					    .withPersistency(myWorkingDir)
                        .open();
// ...


sink.submit(new CustomerChangedEvent(id), "application/vnd.example.event.customerdatachanged+json");


//...
```

To limit the max queue size of the pending send job treject the max buffer size can be reduced by performing `withPersistency(...)`. In this case the `submit` method will be rejected, if the max buffer size is execeeded.    

```
HttpSink sink = HttpSink.target(myUri)
                        .withRetryAfter(Duration.ofSeconds(2), Duration.ofMillis(30), Duration.ofMinutes(5))
					    .withPersistency(myWorkingDir)
                        .withRetryBufferSize(10000)  //max 10000 pending jobs
                        .open();
// ...


sink.submit(new CustomerChangedEvent(id), "application/vnd.example.event.customerdatachanged+json");


//...
```

Furthermore the `submit` method will also be rejected, if some dedicated error codes will be returned by the server. For instance, if the server returns a `400` or a '410', the submission will be rejected by default. This behaviors can be overwritten by using the method `withRejectOnStatus(…)`. In general the defalutreject status list should be OK for you. 

```
HttpSink sink = HttpSink.target(myUri)
                        .withRetryAfter(Duration.ofSeconds(2), Duration.ofMillis(30), Duration.ofMinutes(5))
					    .withPersistency(myWorkingDir)
                        .withRetryBufferSize(10000)  //max 10000 pending
                        .withRejectOnStatus(400, 403, 405, 406, 408, 409, 410, 413, 414, 415, 416, 417) jobs
                        .open();
// ...


sink.submit(new CustomerChangedEvent(id), "application/vnd.example.event.customerdatachanged+json");


//...
```

By calling the `submit` method first it will be tried to perform the query. The submit methods returns either the call is sucessfully or failed for the first time. By calling the `submitAsync` the call returns immediately without waiting for the first response. In this case a `CompletableFuture` is returned   

```
HttpSink sink = HttpSink.target(myUri)
                        .withRetryAfter(Duration.ofSeconds(2), Duration.ofMillis(30), Duration.ofMinutes(5))
					    .withPersistency(myWorkingDir)
                        .open();
// ...


CompletableFuture<Submission> submission = sink.submitAsync(new CustomerChangedEvent(id), "application/vnd.example.event.customerdatachanged+json");


//...
```


