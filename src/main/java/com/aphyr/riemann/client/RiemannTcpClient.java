package com.aphyr.riemann.client;

import com.aphyr.riemann.Proto.Msg;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class RiemannTcpClient extends AbstractRiemannClient {
    protected Socket socket;
    protected final Object socketLock = new Object();
    protected DataOutputStream out;
    protected DataInputStream in;

    public static final int connectTimeout = 1000;
    public static final int readTimeout = 1000;

    public RiemannTcpClient() throws UnknownHostException {
        super();
    }

    public RiemannTcpClient(int port) throws UnknownHostException {
        super(port);
    }

    public RiemannTcpClient(InetSocketAddress server) {
        super(server);
    }

    // This method is not synchronized.
    @Override
    public void sendMessage(Msg message) throws IOException {
        if (message == null) {
            throw new IllegalArgumentException("Null message");
        }
        if (out == null) {
          throw new IOException("No connection to " + super.server.toString());
        }
        out.writeInt(message.getSerializedSize());
        message.writeTo(out);
        out.flush();
    }

    // This method is not synchronized.
    @Override
    public Msg recvMessage() throws IOException {
        // Get length header
        int len = in.readInt();
        if (len < 0) {
            throw new IOException("FUCKED");
        }

        // Get body
        byte[] body = new byte[len];
        in.readFully(body);
        return Msg.parseFrom(body);
    }

    @Override
    public Msg sendRecvMessage(Msg message) throws IOException {
        synchronized (socketLock) {
            sendMessage(message);
            return recvMessage();
        }
    }

    @Override
    public Msg sendMaybeRecvMessage(Msg message) throws IOException {
        return sendRecvMessage(message);
    }

    @Override
    public boolean isConnected() {
        synchronized (socketLock) {
            return this.socket != null && this.socket.isConnected();
        }
    }

    @Override
    public void connect() throws IOException {
        synchronized (socketLock) {
            socket = new Socket();
            socket.connect(super.server, connectTimeout);
            socket.setSoTimeout(readTimeout);
            socket.setTcpNoDelay(true);
            this.out = new DataOutputStream(this.socket.getOutputStream());
            this.in = new DataInputStream(this.socket.getInputStream());
        }
    }

    @Override
    public void disconnect() throws IOException {
        synchronized (socketLock) {
          if (this.out != null) {
            this.out.close();
          }
          if (this.in != null) {
            this.in.close();
          }
          if (this.socket != null) {
            this.socket.close();
          }
        }
    }

    @Override
    public void reconnect() throws IOException {
      synchronized(socketLock) {
        disconnect();
        connect();
      }
    }
}
