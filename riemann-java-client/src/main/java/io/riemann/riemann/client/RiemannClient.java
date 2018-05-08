package io.riemann.riemann.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.riemann.riemann.Proto.Event;
import io.riemann.riemann.Proto.Query;
import io.riemann.riemann.Proto.Msg;
import io.riemann.riemann.client.netty3.TcpTransportFactory;
import io.riemann.riemann.client.netty3.UdpTransportFactory;

// The standard client.
public class RiemannClient implements IRiemannClient {
  // Vars
  public static final MsgValidator validate = new MsgValidator();
  private static TransportFactory<AsynchronousTransport> tcpTransportFactory = new TcpTransportFactory();
  private static TransportFactory<SynchronousTransport> udpTransportFactory = new UdpTransportFactory();

  public static void setTcpTransportFactory(TransportFactory<AsynchronousTransport> tcpTransportFactory) {
    RiemannClient.tcpTransportFactory = tcpTransportFactory;
  }

  public static void setUdpTransportFactory(TransportFactory<SynchronousTransport> udpTransportFactory) {
    RiemannClient.udpTransportFactory = udpTransportFactory;
  }

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
  public static RiemannClient tcp(final InetSocketAddress remoteAddress) throws IOException {
    return wrap(tcpTransportFactory.create(remoteAddress));
  }

  public static RiemannClient tcp(final InetSocketAddress remoteAddress, final InetSocketAddress localAddress) throws IOException {
    return wrap(tcpTransportFactory.create(remoteAddress,localAddress));
  }

  public static RiemannClient tcp(final String remoteHost, final int remotePort) throws IOException{
    return wrap(tcpTransportFactory.create(remoteHost, remotePort));
  }

  public static RiemannClient tcp(final String remoteHost, final int remotePort, final String localHost, final int localPort) throws IOException{
    return wrap(tcpTransportFactory.create(remoteHost, remotePort, localHost, localPort));
  }

  public static RiemannClient tcp(final String remoteHost) throws IOException {
    return wrap(tcpTransportFactory.create(remoteHost));
  }

  public static RiemannClient tcp(final String remoteHost, final String localHost) throws IOException {
    return wrap(tcpTransportFactory.create(remoteHost, localHost));
  }

  public static RiemannClient tcp(final int remotePort) throws IOException {
    return wrap(tcpTransportFactory.create(remotePort));
  }

  // UDP constructors
  // STOP REPEATING YOURSELF KYLE! STOP REPEATING YOURSELF KYLE!
  public static RiemannClient udp(final InetSocketAddress remoteAddress) throws IOException {
    return wrap(udpTransportFactory.create(remoteAddress));
  }

  public static RiemannClient udp(final InetSocketAddress remoteAddress, final InetSocketAddress localAddress) throws IOException {
    return wrap(udpTransportFactory.create(remoteAddress,localAddress));
  }

  public static RiemannClient udp(final String remoteHost, final int remotePort) throws IOException {
    return wrap(udpTransportFactory.create(remoteHost, remotePort));
  }

  public static RiemannClient udp(final String remoteHost, final int remotePort, final String localHost, final int localPort) throws IOException{
    return wrap(udpTransportFactory.create(remoteHost, remotePort, localHost, localPort));
  }

  public static RiemannClient udp(final String remoteHost) {
    return wrap(udpTransportFactory.create(remoteHost));
  }

  public static RiemannClient udp(final String remoteHost, final String localHost) {
    return wrap(udpTransportFactory.create(remoteHost, localHost));
  }

  public static RiemannClient udp(final int remotePort) throws IOException {
    return wrap(udpTransportFactory.create(remotePort));
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
