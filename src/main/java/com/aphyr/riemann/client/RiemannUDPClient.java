package com.aphyr.riemann.client;

import java.io.IOException;
import java.net.*;

import com.aphyr.riemann.Proto.Msg;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class RiemannUDPClient extends AbstractRiemannClient {
    protected static final int MAX_SIZE = 16384;

    public RiemannUDPClient() throws UnknownHostException {
        super();
    }

    public RiemannUDPClient(int port) throws UnknownHostException {
        super(port);
    }

    public RiemannUDPClient(InetSocketAddress server) {
        super(server);
    }

    // Returns true if the message is small enough to be sent.
    public boolean canSendMessage(Msg message) {
        return message.getSerializedSize() <= MAX_SIZE;
    }

    @Override
    public void sendMessage(Msg message) throws IOException, MsgTooLargeException {
        if (message.getSerializedSize() > MAX_SIZE) {
            throw new MsgTooLargeException();
        }

        byte[] buf = message.toByteArray();
        DatagramSocket socket = new DatagramSocket();
        socket.send(new DatagramPacket(buf, buf.length, server));
        socket.close();
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
        return true;
    }

    @Override
    public void connect() throws IOException {
        // do nothing on UDP
    }

    @Override
    public void disconnect() throws IOException {
        // do nothing on UDP
    }
}
