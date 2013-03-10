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

// A client which wraps a transport.
public class RiemannClient extends AbstractRiemannClient implements DualTransport {
  public final DualTransport transport;

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

  // Transport constructors
  public RiemannClient(final SynchronousTransport t) {
    this(new AsynchronizeTransport(t));
  }

  public RiemannClient(final AsynchronousTransport t) {
    this(new SynchronizeTransport(t));
  }

  public RiemannClient(final DualTransport t) {
    this.transport = t;
  }

  // Send and receive messages
  @Override
  public Msg sendRecvMessage(final Msg m) throws IOException {
    return transport.sendRecvMessage(m);
  }

  @Override
  public Msg sendMaybeRecvMessage(final Msg m) throws IOException {
    return transport.sendMaybeRecvMessage(m);
  }

  // Async variants
  @Override
  public IPromise<Msg> aSendRecvMessage(final Msg m) {
    return transport.aSendRecvMessage(m);
  }

  @Override
  public IPromise<Msg> aSendMaybeRecvMessage(final Msg m) {
    return transport.aSendMaybeRecvMessage(m);
  }

  // Lifecycle
  @Override
  public void connect() throws IOException {
    transport.connect();
  }

  @Override
  public boolean isConnected() {
    return transport.isConnected();
  }

  @Override
  public void disconnect() throws IOException {
    transport.disconnect();
  }

  @Override
  public void reconnect() throws IOException {
    transport.reconnect();
  }

  @Override
  public void flush() throws IOException {
    transport.flush();
  }
}
