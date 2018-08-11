package io.riemann.riemann.client;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import io.riemann.riemann.Proto.Msg;

public class SimpleUdpTransport implements SynchronousTransport {

  public static final int DEFAULT_PORT = 5555;

  private volatile DatagramSocket socket;
  private volatile boolean connected = false;

  private final InetSocketAddress remoteAddress;
  private final InetSocketAddress localAddress;

  public SimpleUdpTransport(final InetSocketAddress remoteAddress) {
    this.remoteAddress = remoteAddress;
    this.localAddress = null;
  }

  public SimpleUdpTransport(final InetSocketAddress remoteAddress, final InetSocketAddress localAddress) {
    this.remoteAddress = remoteAddress;
    this.localAddress = localAddress;
  }

  public SimpleUdpTransport(final String host, final int port) throws IOException {
    this(InetSocketAddress.createUnresolved(host, port));
  }

  public SimpleUdpTransport(final String remoteHost, final int remotePort, final String localHost, final int localPort) throws IOException {
    this(InetSocketAddress.createUnresolved(remoteHost, remotePort),
      InetSocketAddress.createUnresolved(localHost, localPort));
  }

  public SimpleUdpTransport(final String host) throws IOException {
    this(host, DEFAULT_PORT);
  }

  public SimpleUdpTransport(final String remoteHost, final String localHost) throws IOException {
    this(remoteHost, DEFAULT_PORT, localHost, 0);
  }

  public SimpleUdpTransport(final int port) throws IOException {
    this(InetAddress.getLocalHost().getHostAddress(), port);
  }

  @Override
  public Msg sendMessage(final Msg msg) throws IOException {
    final byte[] body = msg.toByteArray();
    final DatagramPacket packet = new DatagramPacket(body, body.length, ensureResolved(remoteAddress));
    socket.send(packet);

    return null;
  }

  @Override
  public boolean isConnected() {
    return connected;
  }

  @Override
  public synchronized void connect() throws IOException {
    if (this.localAddress != null){
      socket = new DatagramSocket(ensureResolved(localAddress));
    }else{
      socket = new DatagramSocket();
    }
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

  private static InetSocketAddress ensureResolved(InetSocketAddress maybeUnresolved)
    throws UnknownHostException {
    if (maybeUnresolved.isUnresolved()) {
      return new InetSocketAddress(
        InetAddress.getByName(maybeUnresolved.getHostName()), maybeUnresolved.getPort());
    } else {
      return maybeUnresolved;
    }
  }
}
