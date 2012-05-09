package com.aphyr.riemann.client;
import com.aphyr.riemann.Proto.Msg;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Timer;

// Like RiemannTCPClient, but automatically tries one reconnect/resubmit when an IO error occurs.
public class RiemannRetryingTcpClient extends RiemannTcpClient {
    protected Timer timer = new Timer(true);
    protected Long lastReconnectionAttempt = 0l; // milliseconds
    protected long minimumReconnectInterval = 1; // seconds
    protected boolean reconnecting = false;

    public RiemannRetryingTcpClient() throws UnknownHostException {}

    public RiemannRetryingTcpClient(int port) throws UnknownHostException {
        super(port);
    }

    public RiemannRetryingTcpClient(InetSocketAddress server) {
        super(server);
    }

    @Override
    public Msg sendRecvMessage(Msg m) throws IOException {
        try {
            return super.sendRecvMessage(m);
        } catch (IOException e) {
            reconnect();
            return super.sendRecvMessage(m);
        }
    }

    // Attempts to reconnect. Can be called many times; will only try reconnecting every few seconds.
    // If another thread is reconnecting, or a connection attempt was made too recently, returns immediately.
    public void reconnect() throws IOException {
        Boolean dooo_iiit = false; // paging joedamato
        synchronized(lastReconnectionAttempt) {
            if (reconnecting == false &&
                    (System.currentTimeMillis() - lastReconnectionAttempt) > (minimumReconnectInterval * 1000)) {
                dooo_iiit = true;
                reconnecting = true;
                lastReconnectionAttempt = System.currentTimeMillis();
            }
        }

        if (dooo_iiit) {
            synchronized(this) {
                disconnect();
                connect();
            }
            synchronized (lastReconnectionAttempt) {
                reconnecting = false;
            }
        }
    }
 }
