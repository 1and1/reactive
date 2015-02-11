# reactive


## reactive-http

### reactive JAX-RS Client
Enhanced JAX-RS client which supports Java8 CompletableFuture 

``` java
import net.oneandone.reactive.rest.client.CompletableClient;

CompletableClient client = new CompletableClient(ClientBuilder.newClient());

client.target("http://myservice/hotelbookingsystem/hotels/BUP932432")
      .request()
	  .async()
      .get(HotelRepresentation.class)
      .thenAccept(hotel -> System.out.println(hotel));
```


### reactive JAX-RS Service

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


### reactive Server-Sent Events-based Service

``` java
// ...    
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import net.oneandone.reactive.sse.SseEvent;
import net.oneandone.reactive.sse.servlet.ServletSseEventPublisher;
import net.oneandone.reactive.sse.servlet.ServletSseEventSubscriber;


public class ReactiveSseServlet extends HttpServlet {
    // ...    
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.startAsync();
         
        Publisher<SseEvent> ssePublisher = new ServletSseEventPublisher(req.getInputStream());
        Pipes.newPipe(ssePublisher)
             .map(sseEvent -> KafkaMessage.newMessage().data(sseEvent.getData()))
             .consume(kafkaSubscriber);
    }
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.startAsync();
        resp.setContentType("text/event-stream");
        
        Subscriber<SseEvent> sseSubscriber = new ServletSseEventSubscriber(resp.getOutputStream());
        Pipes.newPipe(kafkaPublisher)
             .map(kafkaMessage -> SseEvent.newEvent().data(kafkaMessage.getData()))
             .consume(sseSubscriber);
    }
}
```



## reactive-pipe
Represents a unidirectional, reactive pipe which is sourced by a [reactive puplisher](http://www.reactive-streams.org) and/or will be consumed by a [reactive subscriber](http://www.reactive-streams.org)

``` java
import net.oneandone.reactive.pipe.Pipes;


Publisher<KafkaMessage> publisher = ...
Subscriber<SseEvent> subscriber = 

Pipes.newPipe(publisher)
     .filter(kafkaMessage -> kafkaMessage.getType() == KafkaMessage.TEXT)
     .map(kafkaMessage -> SSEEvent.newEvent().data(kafkaMessage.getData()))
	 .consume(subscriber);
```
