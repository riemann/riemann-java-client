package com.aphyr.riemann.client;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.aphyr.riemann.Proto.Msg;

public class SimpleUdpTransport implements SynchronousTransport {

  public static final int DEFAULT_PORT = 5555;

  private volatile DatagramSocket socket;
  private volatile boolean connected = false;

  private final InetSocketAddress address;

  private volatile Resolver resolver;

  public volatile boolean cacheDns = true;

  public SimpleUdpTransport(final InetSocketAddress address) {
    this.address = address;
  }

  public SimpleUdpTransport(final String host, final int port) throws IOException {
    this(new InetSocketAddress(host, port));
  }

  public SimpleUdpTransport(final String host) throws IOException {
    this(host, DEFAULT_PORT);
  }

  public SimpleUdpTransport(final int port) throws IOException {
    this(InetAddress.getLocalHost().getHostAddress(), port);
  }

  @Override
  public Msg sendMessage(final Msg msg) throws IOException {
    final byte[] body = msg.toByteArray();
    final DatagramPacket packet = new DatagramPacket(body, body.length, resolver.resolve());
    socket.send(packet);

    return null;
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  @Override
  public synchronized void connect() throws IOException {
    if (cacheDns == true) {
      this.resolver = new CachingResolver(address);
    } else {
      this.resolver = new Resolver(address);
    }
    socket = new DatagramSocket();
    connected = true;
  }

  @Override
  public synchronized void close() {
    try {
      socket.close();
    } finally {
      socket = null;
      connected = false;
    }
  }

  @Override
  public void reconnect() throws IOException {
    close();
    connect();
  }

  @Override
  public void flush() throws IOException {
    // Noop
  }

  @Override
  public Transport transport() {
    return null;
  }
}
