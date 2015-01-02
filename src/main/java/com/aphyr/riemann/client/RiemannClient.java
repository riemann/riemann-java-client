package com.aphyr.riemann.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.List;

import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.Proto.Query;
import com.aphyr.riemann.Proto.Msg;

// The standard client.
public class RiemannClient implements IRiemannClient {
  // Vars
  public static final MsgValidator validate = new MsgValidator();

  // Send an exception over a client.
  public static IPromise<Msg> sendException(final IRiemannClient client,
                                            final String service,
                                            final Throwable t) {
    // Format message and stacktrace
    final StringBuilder desc = new StringBuilder();
    desc.append(t.toString());
    desc.append("\n\n");
    for (StackTraceElement e : t.getStackTrace()) {
      desc.append(e);
      desc.append("\n");
    }

    // Build event and send
    return client.event()
      .service(service)
      .state("error")
      .tag("exception")
      .tag(t.getClass().getSimpleName())
      .description(desc.toString())
      .send();
  }

  // Wrap any transport
  public static RiemannClient wrap(final SynchronousTransport t) {
    return new RiemannClient(t);
  }

  public static RiemannClient wrap(final AsynchronousTransport t) {
    return new RiemannClient(t);
  }

  // TCP constructors
  public static RiemannClient tcp(final InetSocketAddress address) throws IOException {
    return wrap(new TcpTransport(address));
  }

  public static RiemannClient tcp(final String host, final int port) throws IOException{
    return wrap(new TcpTransport(host, port));
  }

  public static RiemannClient tcp(final String host) throws IOException {
    return wrap(new TcpTransport(host));
  }

  public static RiemannClient tcp(final int port) throws IOException {
    return wrap(new TcpTransport(port));
  }

  // UDP constructors
  // STOP REPEATING YOURSELF KYLE! STOP REPEATING YOURSELF KYLE!
  public static RiemannClient udp(final InetSocketAddress address) throws IOException {
    return wrap(new UdpTransport(address));
  }

  public static RiemannClient udp(final String host, final int port) throws IOException {
    return wrap(new UdpTransport(host, port));
  }

  public static RiemannClient udp(final String host) throws IOException {
    return wrap(new UdpTransport(host));
  }

  public static RiemannClient udp(final int port) throws IOException {
    return wrap(new UdpTransport(port));
  }


  // Vars
  public volatile RiemannScheduler scheduler = null;
  public final AsynchronousTransport transport;


  // Transport constructors
  public RiemannClient(final SynchronousTransport t) {
    this(new AsynchronizeTransport(t));
  }

  public RiemannClient(final AsynchronousTransport t) {
    this.transport = t;
  }


  // Create a new event to send over this client
  @Override
  public EventDSL event() {
    return new EventDSL(this);
  }

  // Send and receive messages
  @Override
  public IPromise<Msg> sendMessage(final Msg m) {
    return transport.sendMessage(m).map(validate);
  }

  @Override
  public IPromise<Msg> sendEvent(final Event event) {
    return sendMessage(Msg.newBuilder().addEvents(event).build());
  }

  @Override
  public IPromise<Msg> sendEvents(final Event... events) {
    return sendEvents(Arrays.asList(events));
  }

  @Override
  public IPromise<Msg> sendEvents(final List<Event> events) {
    return sendMessage(Msg.newBuilder().addAllEvents(events).build());
  }

  @Override
  public IPromise<Msg> sendException(final String service, final Throwable t) {
    return RiemannClient.sendException(this, service, t);
  }

  @Override
  public IPromise<List<Event>> query(final String q) {
    return sendMessage(
        Msg.newBuilder()
        .setQuery(Query.newBuilder().setString(q).build())
        .build())
      .map(new Fn2<Msg, List<Event>>() {
        public List<Event> call(final Msg m) {
          return Collections.unmodifiableList(m.getEventsList());
        }
      });
  }


  // Transport lifecycle
  @Override
  public Transport transport() {
    return transport;
  }

  @Override
  public void connect() throws IOException {
    transport.connect();
  }

  @Override
  public boolean isConnected() {
    return transport.isConnected();
  }

  @Override
  public void close() {
    transport.close();
  }

  @Override
  public void reconnect() throws IOException {
    transport.reconnect();
  }

  @Override
  public void flush() throws IOException {
    transport.flush();
  }


  // Returns the scheduler for this client. Creates the scheduler on first use.
  public synchronized RiemannScheduler scheduler() {
    if (scheduler == null) {
      scheduler = new RiemannScheduler(this);
    }
    return scheduler;
  }
}
