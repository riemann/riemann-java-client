package io.riemann.riemann.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public abstract class TransportFactory<T extends Transport> {
    public static final int DEFAULT_PORT = 5555;

    public abstract T create(final InetSocketAddress remoteAddress, final InetSocketAddress localAddress);

    public final T create(final InetSocketAddress remoteAddress) {
        return create(remoteAddress, null);
    }

    public final T create(final String remoteHost, final int remotePort) {
        return create(new InetSocketAddress(remoteHost, remotePort));
    }

    public final T create(final String remoteHost, final int remotePort, final String localHost, final int localPort) {
        return create(
                new InetSocketAddress(remoteHost, remotePort),
                new InetSocketAddress(localHost, localPort));
    }

    public final T create(final String remoteHost) {
        return create(remoteHost, DEFAULT_PORT);
    }

    public final T create(final String remoteHost, final String localHost) {
        return create(remoteHost, DEFAULT_PORT, localHost, 0);
    }

    public final T create(final int remotePort) throws IOException {
        return create(InetAddress.getLocalHost().getHostAddress(), remotePort);
    }

}
