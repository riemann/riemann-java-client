package io.riemann.riemann.client;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import io.riemann.riemann.Proto.Msg;

public class SimpleUdpTransport implements SynchronousTransport {

  public static final int DEFAULT_PORT = 5555;

  private volatile DatagramSocket socket;
  private volatile boolean connected = false;

  private final InetSocketAddress remoteAddress;
  private final InetSocketAddress localAddress;

  private volatile Resolver resolver;
  private volatile Resolver localResolver;

  public volatile boolean cacheDns = true;

  public SimpleUdpTransport(final InetSocketAddress remoteAddress) {
    this.remoteAddress = remoteAddress;
    this.localAddress = null;
  }

  public SimpleUdpTransport(final InetSocketAddress remoteAddress, final InetSocketAddress localAddress) {
    this.remoteAddress = remoteAddress;
    this.localAddress = localAddress;
  }

  public SimpleUdpTransport(final String host, final int port) throws IOException {
    this(new InetSocketAddress(host, port));
  }

  public SimpleUdpTransport(final String remoteHost, final int remotePort, final String localHost, final int localPort) throws IOException {
    this(new InetSocketAddress(remoteHost, remotePort), new InetSocketAddress(localHost, localPort) );
  }

  public SimpleUdpTransport(final String host) throws IOException {
    this(host, DEFAULT_PORT);
  }

  public SimpleUdpTransport(final String remoteHost, final String localHost) throws IOException {
    this(remoteHost, DEFAULT_PORT, localHost, DEFAULT_PORT);
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
      this.resolver = new CachingResolver(remoteAddress);
      if( this.localAddress != null){
        this.localResolver = new CachingResolver(localAddress);
      }
    } else {
      this.resolver = new Resolver(remoteAddress);
      if( this.localAddress != null){
        this.localResolver = new Resolver(localAddress);
      }
    }
    if (this.localAddress != null){
      socket = new DatagramSocket(localResolver.resolve());
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
}
