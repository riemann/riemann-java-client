package com.aphyr.riemann.client;

import com.aphyr.riemann.Proto.Msg;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Timer;

// Like RiemannTCPClient, but automatically tries one reconnect/resubmit when an IO error occurs.
public class RiemannRetryingTcpClient extends RiemannTcpClient {
    protected final Object reconnectionLock = new Object();
    protected long lastReconnectionAttempt = 0l;        // milliseconds
    public volatile long minimumReconnectInterval = 1;  // seconds
    protected volatile boolean reconnecting = false;

    public RiemannRetryingTcpClient() throws UnknownHostException {
      super();
    }

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

    /**
     * @param interval reconnection time in SECONDS
     */
    public void setMinimumReconnectInterval(long interval) {
        if (interval < 0)
            throw new IllegalArgumentException("Invalid interval time");

        minimumReconnectInterval = interval * 1000; // receive in seconds, convert to ms
    }

    // Attempts to reconnect. Can be called many times; will only try
    // reconnecting every few seconds.  If another thread is reconnecting, or a
    // connection attempt was made too recently, returns immediately.
    public void reconnect() throws IOException {
        synchronized (reconnectionLock) {
            long latestAttempt = System.currentTimeMillis() - lastReconnectionAttempt;
            if (!reconnecting && latestAttempt > minimumReconnectInterval) {
                reconnecting = true;
                lastReconnectionAttempt = System.currentTimeMillis();
            } else {
                // no time to reconnect yet or reconnection already in progress
                // release lock and get off here! :)
                return;
            }
        }

        try {
            synchronized (socketLock) {
                disconnect();
                connect();
            }
        } finally {
            synchronized (reconnectionLock) {
                reconnecting = false;
            }
        }

    }
}
