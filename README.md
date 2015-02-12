# reactive-http

## reactive JAX-RS Client
Provides an enhanced JAX-RS client which supports Java8 `CompletableFuture`. This client supports CompletableFuture methods such as thenAccept(...) consuming a function which will be executed as soon as the http response is received. The thenAccept(..) call returns immediately without waiting for the http response         

``` java
import net.oneandone.reactive.rest.client.CompletableClient;

CompletableClient client = new CompletableClient(ClientBuilder.newClient());

client.target("http://myservice/hotelbookingsystem/hotels/BUP932432")
      .request()
	  .async()
      .get(HotelRepresentation.class)
      .thenAccept(hotel -> System.out.println(hotel));
```


## reactive JAX-RS Service
Provides convenience artifacts such as `ResultConsumer`

``` java
// ...
import static net.oneandone.reactive.rest.container.ResultConsumer.writeTo;


@Path("/hotels")
public class HotelsResource {
    // ...    
    
    
    @Path("/{id}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public void retrieveHotelDescriptionAsync(@PathParam("id") long id, @Suspended AsyncResponse resp) {
        hotelDao.readHotelAsync(id)
                .thenApply(hotel -> new HotelRepresentation(hotel.getName(), hotel.getDescription()))
                .whenComplete(writeTo(resp));
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
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.startAsync();
         
        Publisher<ServerSentEvent> ssePublisher = new ServletSsePublisher(req.getInputStream());
        Pipes.newPipe(ssePublisher)
             .map(sseEvent -> KafkaMessage.newMessage().data(sseEvent.getData()))
             .consume(kafkaSubscriber);
    }
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.startAsync();
        resp.setContentType("text/event-stream");
        
        Subscriber<ServerSentEvent> sseSubscriber = new ServletSseSubscriber(resp.getOutputStream());
        Pipes.newPipe(kafkaPublisher)
             .map(kafkaMessage -> ServerSentEvent.newEvent().data(kafkaMessage.getData()))
             .consume(sseSubscriber);
    }
}
```



# reactive-pipe
Provides an unidirectional, reactive `Pipe` which is sourced by a [reactive publisher](http://www.reactive-streams.org) and/or will be consumed by a [reactive subscriber](http://www.reactive-streams.org)

``` java
import net.oneandone.reactive.pipe.Pipes;


Publisher<KafkaMessage> kafkaPublisher = ...
Subscriber<ServerSentEvent> sseSubscriber = ...

Pipes.newPipe(kafkaPublisher)
     .filter(kafkaMessage -> kafkaMessage.getType() == KafkaMessage.TEXT)
     .map(kafkaMessage -> ServerSentEvent.newEvent().data(kafkaMessage.getData()))
	 .consume(sseSubscriber);
```
