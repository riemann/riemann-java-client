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

// Wraps a Transport.
public class RiemannClient extends AbstractRiemannClient {
  public final SynchronousTransport transport;

  // TCP constructors
  public static RiemannClient tcp(final InetSocketAddress address) throws IOException {
    return new RiemannClient(new TcpTransport(address));
  }

  public static RiemannClient tcp(final String host, final int port) throws IOException{
    return new RiemannClient(new TcpTransport(host, port));
  }

  public static RiemannClient tcp(final String host) throws IOException {
    return new RiemannClient(new TcpTransport(host));
  }

  public static RiemannClient tcp(final int port) throws IOException {
    return new RiemannClient(new TcpTransport(port));
  }

  // Transport constructors
  public RiemannClient(final SynchronousTransport t) {
    this.transport = t;
  }

  public RiemannClient(final AsynchronousTransport t) {
    this.transport = new SynchronizeTransport(t);
  }

  // Send and receive messages
  public Msg sendRecvMessage(final Msg m) throws IOException {
    return transport.sendRecvMessage(m);
  }

  public Msg sendMaybeRecvMessage(final Msg m) throws IOException {
    return transport.sendMaybeRecvMessage(m);
  }

  // Lifecycle
  public void connect() throws IOException {
    transport.connect();
  }

  public boolean isConnected() {
    return transport.isConnected();
  }

  public void disconnect() throws IOException {
    transport.disconnect();
  }

  public void reconnect() throws IOException {
    transport.reconnect();
  }

  public void flush() throws IOException {
    transport.flush();
  }
}
