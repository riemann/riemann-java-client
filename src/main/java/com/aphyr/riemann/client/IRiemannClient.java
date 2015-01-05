package com.aphyr.riemann.client;

import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.Proto.Query;
import com.aphyr.riemann.Proto.Msg;
import java.util.List;

// The core functionality of any client.

public interface IRiemannClient extends AsynchronousTransport {
  // Send any number of events asynchronously. Returns a promise of a response
  // Msg.
  IPromise<Msg> sendEvent(final Event event);
  IPromise<Msg> sendEvents(final Event... events);
  IPromise<Msg> sendEvents(final List<Event> events);

  // Send an exception as an event.
  IPromise<Msg> sendException(final String service, final Throwable t);

  // Query the server for all events matching query. Returns a promise of a
  // list of events.
  IPromise<List<Event>> query(final String q);

  // Create an EventDSL bound to this client
  EventDSL event();
}
