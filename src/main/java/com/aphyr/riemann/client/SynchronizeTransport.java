package com.aphyr.riemann.client;

// Wraps an asynchronous transport with a concurrent synchronous layer.

import com.aphyr.riemann.Proto.Msg;
import java.io.IOException;

public class SynchronizeTransport implements SynchronousTransport {
  public final AsynchronousTransport transport;
  
  public SynchronizeTransport(final AsynchronousTransport transport) {
    this.transport = transport;
  }

  public void sendMessage(final Msg msg) throws IOException {
    transport.aSendMessage(msg);
  }

  public Msg sendRecvMessage(final Msg msg) throws IOException {
    return transport.aSendRecvMessage(msg).await();
  }

  public Msg sendMaybeRecvMessage(final Msg msg) throws IOException {
    return transport.aSendMaybeRecvMessage(msg).await();
  }

  public boolean isConnected() {
    return transport.isConnected();
  }

  public void connect() throws IOException {
    transport.connect();
  }

  public void disconnect() throws IOException {
    transport.disconnect();
  }

  public void reconnect() throws IOException {
    transport.reconnect();
  }

  public void flush() throws IOException {
    transport.flush();
  }
}
