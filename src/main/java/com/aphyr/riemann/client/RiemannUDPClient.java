package com.aphyr.riemann.client;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.*;

import com.aphyr.riemann.Proto.Msg;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class RiemannUDPClient extends AbstractRiemannClient {
    protected DatagramSocket socket;
    protected int max_size = 16384;

    public RiemannUDPClient() throws UnknownHostException {
        super();
    }

    public RiemannUDPClient(int port) throws UnknownHostException {
        super(port);
    }

    public RiemannUDPClient(InetSocketAddress server) {
        super(server);
    }

    @Override
    public void sendMessage(Msg message) throws IOException, MsgTooLargeException {
        if (message.getSerializedSize() > max_size) {
            throw new MsgTooLargeException();
        }

        byte[] buf = message.toByteArray();
        synchronized(this) {
            socket.send(new DatagramPacket(buf, buf.length, server));
        }
    }

    @Override
    public Msg recvMessage() throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public Msg sendRecvMessage(Msg message) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public Msg sendMaybeRecvMessage(Msg message) throws IOException, MsgTooLargeException {
        sendMessage(message);
        return null;
    }

    @Override
    public boolean isConnected() {
        return this.socket != null && this.socket.isConnected();
    }

    @Override
    public void connect() throws IOException {
        synchronized (this) {
            socket = new DatagramSocket();
        }
    }

    @Override
    public void disconnect() throws IOException {
        synchronized(this) {
            socket.close();
        }
    }
}
