package com.aphyr.riemann.client;

// Wraps an asynchronous transport with a concurrent synchronous layer.

import com.aphyr.riemann.Proto.Msg;
import java.io.IOException;

public class SynchronizeTransport implements DualTransport {
  public final AsynchronousTransport transport;

  public SynchronizeTransport(final AsynchronousTransport transport) {
    this.transport = transport;
  }

  // Asynchronous layer
  public IPromise<Msg> aSendRecvMessage(final Msg msg) {
    return transport.aSendRecvMessage(msg);
  }

  public IPromise<Msg> aSendMaybeRecvMessage(final Msg msg) {
    return transport.aSendMaybeRecvMessage(msg);
  }

  // Synchronous layer
  public Msg sendRecvMessage(final Msg msg) throws IOException {
    return transport.aSendRecvMessage(msg).deref();
  }

  public Msg sendMaybeRecvMessage(final Msg msg) throws IOException {
    return transport.aSendMaybeRecvMessage(msg).deref();
  }

  // Lifecycle
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
