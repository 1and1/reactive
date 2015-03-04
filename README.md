# reactive-http
``` java
<dependency>
    <groupId>net.oneandone.reactive</groupId>
    <artifactId>reactive-http</artifactId>
    <version>0.5</version>
</dependency>
```

## reactive JAX-RS Client
The `RxClient` extends the [JAX-RX 2.0 Client](http://docs.oracle.com/javaee/7/api/javax/ws/rs/client/Client.html) by supporting an additional method `rx()`. This method returns a reactive invoker which supports  Java8 `CompletableFuture`.The CompletableFuture provides methods such as thenAccept(...) which consumes a function which will be executed, if the http response is received. The thenAccept(..) call returns immediately without waiting for the http response         

``` java
import net.oneandone.reactive.rest.client.RxClient;

RxClient client = new RxClient(ClientBuilder.newClient());

client.target("http://myservice/hotelbookingsystem/hotels/BUP932432")
      .request()
	  .rx()        
      .get(HotelRepresentation.class)
      .thenAccept(hotel -> System.out.println(hotel));
```


## reactive JAX-RS Service

### `ResultConsumer`

``` java
// ...


@Path("/hotels")
public class HotelsResource {
    // ...    
    
    
    @Path("/{id}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void retrieveHotelDescriptionAsync(@PathParam("id") long id, @Suspended AsyncResponse response) {
        hotelDao.readHotelAsync(id)
                .thenApply(hotel -> new HotelRepresentation(hotel.getName(), hotel.getDescription()))
                .whenComplete(ResultConsumer.writeTo(response));
    }
}
```


### `PublisherConsumer`
Provides convenience artifacts such as `PublisherConsumer`

The `writeFirstTo(...)` reads the first element of the publisher and writes this element to the HTTP response sequence. 
If the publisher does not return an element, a 204 No Content is returned.

``` java
// ...


@Path("/hotels")
public class HotelsResource {
    // ...    
    
    @Path("/{id}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void retrieveHotelDescriptionAsync(@PathParam("id") long id, @Suspended AsyncResponse response) {
        hotelDao.readHotelAsync(id)
                .thenApply(publisher -> RxReactiveStreams.toObservable(publisher))
                .thenApply(observable -> observable.map(hotel -> new HotelRepresentation(hotel.getName(), hotel.getDescription()))
                .thenApply(observable -> RxReactiveStreams.toPublisher(observable))
                .whenComplete(PublisherConsumer.writeFirstTo(response)); 
    }
}
```


The `writeSingleTo(...)` reads the first element of the publisher and writes this element to the HTTP response sequence. 
If the publisher does not return an element, a 404 Not Found is returned. If the publisher supports more than 1 element a
409 Conflict error will be returned

``` java
// ...


@Path("/hotels")
public class HotelsResource {
    // ...    
    
    @Path("/{id}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void retrieveHotelDescriptionAsync(@PathParam("id") long id, @Suspended AsyncResponse response) {
        hotelDao.readHotelAsync(id)
                .thenApply(publisher -> RxReactiveStreams.toObservable(publisher))
                .thenApply(observable -> observable.map(hotel -> new HotelRepresentation(hotel.getName(), hotel.getDescription()))
                .thenApply(observable -> RxReactiveStreams.toPublisher(observable))
                .whenComplete(PublisherConsumer.writeSingleTo(response));  
    }
}
```


## reactive Server-Sent Events-based Service
Provides full async/non-blocking Servlet 3.1 based reactive [Publisher](http://www.reactive-streams.org) and [Subscriber](http://www.reactive-streams.org) 

``` java
// ...    
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import net.oneandone.reactive.sse.ServerSentEvent;
import net.oneandone.reactive.sse.servlet.ServletSsePublisher;
import net.oneandone.reactive.sse.servlet.ServletSseSubscriber;


public class ReactiveSseServlet extends HttpServlet {
    private final Publisher<KafkaMessage> kafkaPublisher = ...
	private final Subscriber<KafkaMessage> kafkaSubscriber = ... 
    // ...    
    
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        request.startAsync();
         
        Publisher<ServerSentEvent> ssePublisher = new ServletSsePublisher(request.getInputStream());
        Pipes.newPipe(ssePublisher)
             .map(sseEvent -> KafkaMessage.newMessage().data(sseEvent.getData()))
             .consume(kafkaSubscriber);
    }
    
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        request.startAsync();
        response.setContentType("text/event-stream");
        
        Subscriber<ServerSentEvent> sseSubscriber = new ServletSseSubscriber(response.getOutputStream());
        Pipes.newPipe(kafkaPublisher)
             .map(kafkaMessage -> ServerSentEvent.newEvent().data(kafkaMessage.getData()))
             .consume(sseSubscriber);
    }
}
```

