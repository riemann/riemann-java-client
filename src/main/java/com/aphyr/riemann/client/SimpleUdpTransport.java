package com.aphyr.riemann.client;

import com.google.protobuf.MessageLite;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;

public class SimpleUdpTransport implements SynchronousTransport {

  public static final int DEFAULT_PORT = 5555;

  private volatile DatagramSocket socket;
  private volatile boolean connected = false;

  private final InetSocketAddress address;

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
  public com.aphyr.riemann.Proto.Msg sendMaybeRecvMessage(final com.aphyr.riemann.Proto.Msg msg) throws IOException {
    final byte[] body = msg.toByteArray();
    final DatagramPacket packet = new DatagramPacket(body, body.length, address);
    socket.send(packet);
    return null;
  }

  @Override
  public com.aphyr.riemann.Proto.Msg sendRecvMessage(final com.aphyr.riemann.Proto.Msg msg) throws IOException {
    throw new UnsupportedOperationException("UDP transport doesn't support receiving messages");
  }

  @Override
  public boolean isConnected() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public synchronized void connect() throws IOException {
    socket = new DatagramSocket();
    connected = true;
  }

  @Override
  public synchronized void disconnect() throws IOException {
    try {
      socket.close();
    } finally {
      socket = null;
      connected = false;
    }
  }

  @Override
  public void reconnect() throws IOException {
    disconnect();
    connect();
  }

  @Override
  public void flush() throws IOException {
    // Noop
  }
}
