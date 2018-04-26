package io.riemann.riemann.client;

import io.riemann.riemann.Proto.Msg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractTcpTransport implements AsynchronousTransport {
  // Logger
  public final Logger logger = LoggerFactory.getLogger(AbstractTcpTransport.class);

  // I AM A STATE MUSHEEN
  public enum State {
    DISCONNECTED,
     CONNECTING,
      CONNECTED,
      DISCONNECTING
  }

  // STATE STATE STATE
  public volatile State state = State.DISCONNECTED;
  public volatile Semaphore writeLimiter = new Semaphore(8192);

  // Configuration
  public final AtomicInteger writeLimit     = new AtomicInteger(8192);
  public final AtomicLong    reconnectDelay = new AtomicLong(5000);
  public final AtomicInteger connectTimeout = new AtomicInteger(5000);
  public final AtomicInteger writeTimeout   = new AtomicInteger(5000);
  public final AtomicInteger writeBufferHigh = new AtomicInteger(1024 * 64);
  public final AtomicInteger writeBufferLow  = new AtomicInteger(1024 * 8);
  public final AtomicBoolean cacheDns = new AtomicBoolean(true);
  public final InetSocketAddress remoteAddress;
  public final InetSocketAddress localAddress;

  public volatile ExceptionReporter exceptionReporter = new ExceptionReporter() {
    public void reportException(final Throwable t) {
      // By default, don't spam the logs.
    }
  };

  public void setExceptionReporter(final ExceptionReporter exceptionReporter) {
    this.exceptionReporter = exceptionReporter;
  }

  protected AbstractTcpTransport(final InetSocketAddress remoteAddress, final InetSocketAddress localAddress) {
    this.remoteAddress = remoteAddress;
    this.localAddress = localAddress;
  }

  // Set the number of outstanding writes allowed at any time.
  public synchronized AbstractTcpTransport setWriteBufferLimit(final int limit) {
    if (isConnected()) {
      throw new IllegalStateException("can't modify the write buffer limit of a connected transport; please set the limit before connecting");
    }

    writeLimit.set(limit);
    writeLimiter = new Semaphore(limit);
    return this;
  }

  @Override
  public boolean isConnected() {
    // Are we in state connected?
    if (state != State.CONNECTED) {
      return false;
    }

    return checkConnected();
  }

  protected abstract boolean checkConnected();

  @Override
  // Does nothing if not currently disconnected.
  public synchronized void connect() throws IOException {
    if (state != State.DISCONNECTED) {
      return;
    }
    ;
    state = State.CONNECTING;

    ConnectionResult result = doConnect();

    // At this point we consider the client "connected"--even though the
    // connection may have failed. The channel will continue to initiate
    // reconnect attempts in the background.
    state = State.CONNECTED;

    // We'll throw an exception so users can pretend this call is synchronous
    // (and log errors as appropriate) but the client might succeed later.
    if (!result.success) {
      throw new IOException("Connection failed", result.cause);
    }
  }

  protected static class ConnectionResult {
    public final boolean success;
    public final Throwable cause;

    public ConnectionResult(boolean success, Throwable cause) {
      this.success = success;
      this.cause = cause;

    }
  }

  protected abstract ConnectionResult doConnect() throws IOException;

  @Override
  public void close() {
    close(false);
  }

  public synchronized void close(boolean force) {
    if (!(force || state == State.CONNECTED)) {
      return;
    }

    doClose();
  }

  protected abstract void doClose();

  @Override
  public synchronized void reconnect() throws IOException {
    close();
    connect();
  }

  @Override
  public void flush() throws IOException {
  // TODO: in Netty 4
  //    channels.flush().sync();
  }

  // Write a message to any handler and return a promise to be fulfilled by
  // the corresponding response Msg.
  @Override
  public IPromise<Msg> sendMessage(final Msg msg) {
    return sendMessage(msg, new Promise<Msg>());
  }

  // Write a message to any available handler, fulfilling a specific promise.
  public Promise<Msg> sendMessage(final Msg msg, final Promise<Msg> promise) {
    if (state != State.CONNECTED) {
      promise.deliver(new IOException("client not connected"));
      return promise;
    }

    final Semaphore limiter = writeLimiter;
    final Write write = new Write(msg, promise);

    boolean sent;
    // Reserve a slot in the queue
    if (limiter.tryAcquire()) {

      sent = doSend(write, new Runnable() {
        @Override
        public void run() {
          limiter.release();
        }
      });

      if (!sent) {
        // No channels available, release the slot.
        limiter.release();
        promise.deliver(new IOException("no channels available"));
      }

    } else {

      // Buffer's full.
      promise.deliver(
              new OverloadedException(
                      "client write buffer is full: " +
                              writeLimiter.availablePermits() + " / " +
                              writeLimit.get() + " messages."));
    }

    return promise;

  }

  protected abstract boolean doSend(Write write, Runnable sentCallback);

  @Override
  public Transport transport() {
    return null;
  }
}
