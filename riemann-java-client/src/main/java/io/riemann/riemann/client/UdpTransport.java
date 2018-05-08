package io.riemann.riemann.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * @deprecated use {@link io.riemann.riemann.client.netty3.UdpTransport} class directly
 * or a factory method from {@link RiemannClient}
 */
public class UdpTransport extends io.riemann.riemann.client.netty3.UdpTransport {

  public UdpTransport(final InetSocketAddress remoteAddress) {
      super(remoteAddress, null);
  }

  public UdpTransport(final InetSocketAddress remoteAddress, final InetSocketAddress localAddress) {
    super(remoteAddress, localAddress);
  }

  public UdpTransport(final String host, final int port) throws IOException {
    this(new InetSocketAddress(host, port));
  }

  public UdpTransport(final String remoteHost, final int remotePort, final String localHost, final int localPort) throws IOException {
    this(new InetSocketAddress(remoteHost, remotePort),new InetSocketAddress(localHost, localPort) );
  }

  public UdpTransport(final String remoteHost) throws IOException {
    this(remoteHost, DEFAULT_PORT);
  }

  public UdpTransport(final String remoteHost, final String localHost) throws IOException {
    this(remoteHost, DEFAULT_PORT, localHost, 0);
  }

  public UdpTransport(final int port) throws IOException {
    this(InetAddress.getLocalHost().getHostAddress(), port);
  }
}
