package com.aphyr.riemann.client;

// Wraps an asynchronous transport with a concurrent synchronous layer.

import com.aphyr.riemann.Proto.Msg;
import java.io.IOException;

public class AsynchronizeTransport implements AsynchronousTransport {
  public final SynchronousTransport transport;

  public AsynchronizeTransport(final SynchronousTransport transport) {
    this.transport = transport;
  }

  // Asynchronous path
  public Promise<Msg> sendMessage(final Msg msg) {
    final Promise<Msg> p = new Promise<Msg>();

    try {
      final Msg response = transport.sendMessage(msg);
      if (response == null) {
        p.deliver(new UnsupportedOperationException(
              transport.toString() + " doesn't support receiving messages."));
      } else {
        p.deliver(response);
      }
    } catch (IOException e) {
      p.deliver(e);
    } catch (RuntimeException e) {
      p.deliver(e);
    }
    return p;
  }

  @Override
  public SynchronousTransport transport() {
    return transport;
  }

  // Lifecycle
  @Override
  public boolean isConnected() {
    return transport.isConnected();
  }

  @Override
  public void connect() throws IOException {
    transport.connect();
  }

  @Override
  public void close() {
    transport.close();
  }

  @Override
  public void reconnect() throws IOException {
    transport.reconnect();
  }

  @Override
  public void flush() throws IOException {
    transport.flush();
  }
}
