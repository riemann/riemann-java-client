package io.riemann.riemann.client.netty3;

import io.riemann.riemann.client.SynchronousTransport;
import io.riemann.riemann.client.TransportFactory;

import java.net.InetSocketAddress;

public class UdpTransportFactory extends TransportFactory<SynchronousTransport> {

    @Override
    public UdpTransport create(InetSocketAddress remoteAddress, InetSocketAddress localAddress) {
        return new UdpTransport(remoteAddress, localAddress);
    }
}
