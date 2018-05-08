package io.riemann.riemann.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * @deprecated use {@link io.riemann.riemann.client.netty3.TcpTransport} class directly
 * or a factory method from {@link RiemannClient}
 */
public class TcpTransport extends io.riemann.riemann.client.netty3.TcpTransport {

  public TcpTransport(final InetSocketAddress remoteAddress) {
      super(remoteAddress, null);
  }

  public TcpTransport(final InetSocketAddress remoteAddress, final InetSocketAddress localAddress) {
    super(remoteAddress, localAddress);
  }

  public TcpTransport(final String remoteHost, final int remotePort) throws IOException {
    this(new InetSocketAddress(remoteHost, remotePort));
  }

  public TcpTransport(final String remoteHost, final int remotePort, final String localHost, final int localPort) throws IOException {
    this(new InetSocketAddress(remoteHost, remotePort),new InetSocketAddress(localHost, localPort) );
  }

  public TcpTransport(final String remoteHost) throws IOException {
    this(remoteHost, DEFAULT_PORT);
  }

  public TcpTransport(final String remoteHost, final String localHost) throws IOException {
    this(remoteHost, DEFAULT_PORT, localHost, 0);
  }

  public TcpTransport(final int remotePort) throws IOException {
    this(InetAddress.getLocalHost().getHostAddress(), remotePort);
  }
}
