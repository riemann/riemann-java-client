package com.aphyr.riemann.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import com.aphyr.riemann.Proto.Msg;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

// A hybrid UDP/TCP client.
public class RiemannClient extends AbstractRiemannClient {
    public RiemannRetryingTcpClient tcp;
    public RiemannUDPClient udp;

    public RiemannClient(InetSocketAddress server) {
        super(server);
        udp = new RiemannUDPClient(server);
        tcp = new RiemannRetryingTcpClient(server);
    }

    public RiemannClient(final int port) throws UnknownHostException {
       this(new InetSocketAddress(InetAddress.getLocalHost(), port));
    }

    public RiemannClient() throws UnknownHostException {
        this(new InetSocketAddress(InetAddress.getLocalHost(), DEFAULT_PORT));
    }

    @Override
    public void sendMessage(Msg message) {
        throw new NotImplementedException();
    }

    @Override
    public Msg recvMessage() {
        throw new NotImplementedException();
    }

    @Override
    public Msg sendRecvMessage(Msg message) throws IOException {
        return tcp.sendRecvMessage(message);
    }

    @Override
    // Attempts to dispatch the message quickly via UDP, then falls back to TCP.
    public Msg sendMaybeRecvMessage(Msg message) throws IOException {
        try {
            return udp.sendMaybeRecvMessage(message);
        } catch (MsgTooLargeException e) {
            return tcp.sendMaybeRecvMessage(message);
        }
    }

    @Override
    public boolean isConnected() {
        return(udp.isConnected() && tcp.isConnected());
    }

    @Override
    public void connect() throws IOException {
        synchronized(this) {
            udp.connect();
            tcp.connect();
        }
    }

    @Override
    public void disconnect() throws IOException {
        synchronized(this) {
            udp.disconnect();
            tcp.disconnect();
        }
    }
}