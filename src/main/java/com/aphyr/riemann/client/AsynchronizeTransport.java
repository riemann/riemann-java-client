package com.aphyr.riemann.client;

// Wraps an asynchronous transport with a concurrent synchronous layer.

import com.aphyr.riemann.Proto.Msg;
import java.io.IOException;

public class AsynchronizeTransport implements DualTransport {
  public final SynchronousTransport transport;

  public AsynchronizeTransport(final SynchronousTransport transport) {
    this.transport = transport;
  }

  // Asynchronous path
  public Promise<Msg> aSendRecvMessage(final Msg msg) {
    final Promise<Msg> p = new Promise<Msg>();
    try {
      p.deliver(transport.sendRecvMessage(msg));
    } catch (IOException e) {
      p.deliver(e);
    } catch (RuntimeException e) {
      p.deliver(e);
    }
    return p;
  }

  public Promise<Msg> aSendMaybeRecvMessage(final Msg msg) {
    final Promise<Msg> p = new Promise<Msg>();
    try {
      p.deliver(transport.sendMaybeRecvMessage(msg));
    } catch (IOException e) {
      p.deliver(e);
    } catch (RuntimeException e) {
      p.deliver(e);
    }
    return p;
  }
  
  // Synchronous path
  public Msg sendRecvMessage(final Msg msg) throws IOException {
    return transport.sendRecvMessage(msg);
  }

  public Msg sendMaybeRecvMessage(final Msg msg) throws IOException {
    return transport.sendMaybeRecvMessage(msg);
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
