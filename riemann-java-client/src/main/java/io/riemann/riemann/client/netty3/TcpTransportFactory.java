package io.riemann.riemann.client.netty3;

import io.riemann.riemann.client.AsynchronousTransport;
import io.riemann.riemann.client.TransportFactory;

import java.net.InetSocketAddress;

public class TcpTransportFactory extends TransportFactory<AsynchronousTransport> {

    @Override
    public TcpTransport create(InetSocketAddress remoteAddress, InetSocketAddress localAddress) {
        return new TcpTransport(remoteAddress, localAddress);
    }
}
