package io.riemann.riemann.client;

import io.riemann.riemann.Proto.Msg;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractUdpTransport implements SynchronousTransport {
  // For writes we don't care about
  public static final Promise<Msg> blackhole =
    new Promise<Msg>();


  // I AM A STATE MUSHEEN
  public enum State {
    DISCONNECTED,
     CONNECTING,
      CONNECTED,
      DISCONNECTING
  }

  // STATE STATE STATE
  public volatile State state = State.DISCONNECTED;



  // Configuration
  public final AtomicLong reconnectDelay = new AtomicLong(5000);
  public final AtomicLong connectTimeout = new AtomicLong(5000);
  // Changes to this value are applied only on reconnect.
  public final AtomicInteger sendBufferSize = new AtomicInteger(16384);
  public final AtomicBoolean cacheDns = new AtomicBoolean(true);
  public final InetSocketAddress remoteAddress;
  public final InetSocketAddress localAddress;

  public volatile ExceptionReporter exceptionReporter = new ExceptionReporter() {
    @Override
    public void reportException(final Throwable t) {
    t.printStackTrace();
    }
  };

  public void setExceptionReporter(final ExceptionReporter exceptionReporter) {
    this.exceptionReporter = exceptionReporter;
  }

  public AbstractUdpTransport(final InetSocketAddress remoteAddress, final InetSocketAddress localAddress) {
    this.remoteAddress = remoteAddress;
    this.localAddress = localAddress;
  }

  @Override
  public boolean isConnected() {
    // Are we in state connected?
    return state == State.CONNECTED;
  }

  @Override
  // Does nothing if not currently disconnected.
  public synchronized void connect() throws IOException {
    if (state != State.DISCONNECTED) {
      return;
    }
    state = State.CONNECTING;

    ConnectionResult result = doConnect();

    // Check for errors.
    if (!result.success) {
      close(true);
      throw new IOException("Connection failed", result.cause);
    }

    // Done
    state = State.CONNECTED;
  }

  protected abstract ConnectionResult doConnect();

  @Override
  public void close() {
    close(false);
  }

  public synchronized void close(boolean force) {
    if (!(force || state == State.CONNECTED)) {
      return;
    }

    doStop();

    state = State.DISCONNECTED;
  }

  protected abstract void doStop();

  @Override
  public void reconnect() throws IOException {
    close();
    connect();
  }

  // An Noop
  @Override
  public void flush() throws IOException {
  }

  @Override
  public Msg sendMessage(final Msg msg) {
    doSend(msg);
    return null;
  }

  protected abstract void doSend(Msg msg);

  @Override
  public Transport transport() {
    return null;
  }

  protected static class ConnectionResult {
    public final boolean success;
    public final Throwable cause;

    public ConnectionResult(boolean success, Throwable cause) {
      this.success = success;
      this.cause = cause;

    }
  }
}
