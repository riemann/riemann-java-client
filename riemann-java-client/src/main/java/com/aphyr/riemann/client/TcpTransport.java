package com.aphyr.riemann.client;

import com.aphyr.riemann.Proto.Msg;
import java.io.*;
import java.net.*;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.handler.codec.frame.*;
import org.jboss.netty.handler.codec.protobuf.*;
import org.jboss.netty.handler.ssl.*;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpTransport implements AsynchronousTransport {
  // Logger
  public final Logger logger = LoggerFactory.getLogger(TcpTransport.class);

  // Shared pipeline handlers
  public static final ProtobufDecoder pbDecoder =
    new ProtobufDecoder(Msg.getDefaultInstance());
  public static final ProtobufEncoder pbEncoder =
    new ProtobufEncoder();
  public static final LengthFieldPrepender frameEncoder =
    new LengthFieldPrepender(4);

  public static final int DEFAULT_PORT = 5555;

  // I AM A STATE MUSHEEN
  public enum State {
    DISCONNECTED,
     CONNECTING,
      CONNECTED,
      DISCONNECTING
  }

  // STATE STATE STATE
  public volatile State state = State.DISCONNECTED;
  public final ChannelGroup channels = new DefaultChannelGroup();
  public volatile Timer timer;
  public volatile ClientBootstrap bootstrap;
  public volatile Semaphore writeLimiter = new Semaphore(8192);

  // Configuration
  public final AtomicInteger writeLimit     = new AtomicInteger(8192);
  public final AtomicLong    reconnectDelay = new AtomicLong(5000);
  public final AtomicInteger connectTimeout = new AtomicInteger(5000);
  public final AtomicInteger writeTimeout   = new AtomicInteger(5000);
  public final AtomicInteger writeBufferHigh = new AtomicInteger(1024 * 64);
  public final AtomicInteger writeBufferLow  = new AtomicInteger(1024 * 8);
  public final AtomicBoolean cacheDns = new AtomicBoolean(true);
  public final InetSocketAddress address;
  public final AtomicReference<SSLContext> sslContext =
    new AtomicReference<SSLContext>();

  public volatile ExceptionReporter exceptionReporter = new ExceptionReporter() {
    public void reportException(final Throwable t) {
      // By default, don't spam the logs.
    }
  };

  public void setExceptionReporter(final ExceptionReporter exceptionReporter) {
    this.exceptionReporter = exceptionReporter;
  }

  public TcpTransport(final InetSocketAddress address) {
    this.address = address;
  }

  public TcpTransport(final String host, final int port) throws IOException {
    this(new InetSocketAddress(host, port));
  }

  public TcpTransport(final String host) throws IOException {
    this(host, DEFAULT_PORT);
  }

  public TcpTransport(final int port) throws IOException {
    this(InetAddress.getLocalHost().getHostAddress(), port);
  }


  // Set the number of outstanding writes allowed at any time.
  public synchronized TcpTransport setWriteBufferLimit(final int limit) {
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

    // Is at least one channel connected?
    for (Channel ch : channels) {
      if (ch.isConnected()) {
        return true;
      }
    }

    return false;
  }

  // Builds a new SSLHandler
  public SslHandler sslHandler() {
    final SSLContext context = sslContext.get();
    if (context == null) {
      return null;
    }

    final SSLEngine engine = context.createSSLEngine();
    engine.setUseClientMode(true);

    final SslHandler handler = new SslHandler(engine);
    handler.setEnableRenegotiation(false);
    handler.setIssueHandshake(true);

    return handler;
  }

  @Override
  // Does nothing if not currently disconnected.
  public synchronized void connect() throws IOException {
    if (state != State.DISCONNECTED) {
      return;
    };
    state = State.CONNECTING;

    // Set up channel factory
    final ChannelFactory channelFactory = new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool());

    // Timer
    timer = new HashedWheelTimer();

    // Create bootstrap
    bootstrap = new ClientBootstrap(channelFactory);

    // Set up pipeline factory.
    bootstrap.setPipelineFactory(
        new ChannelPipelineFactory() {
          public ChannelPipeline getPipeline() {
            final ChannelPipeline p = Channels.pipeline();

            // Reconnections
            p.addLast("reconnect", new ReconnectHandler(
                bootstrap,
                timer,
                reconnectDelay,
                TimeUnit.MILLISECONDS));

            // TLS
            final SslHandler sslHandler = sslHandler();
            if (sslHandler != null) {
              p.addLast("tls", sslHandler);
            }

            // Normal codec
            p.addLast("frame-decoder", new LengthFieldBasedFrameDecoder(
                Integer.MAX_VALUE, 0, 4, 0, 4));
            p.addLast("frame-encoder", frameEncoder);
            p.addLast("protobuf-decoder", pbDecoder);
            p.addLast("protobuf-encoder", pbEncoder);
            p.addLast("channelgroups", new ChannelGroupHandler(channels));
            p.addLast("handler", new TcpHandler(exceptionReporter));

            return p;
          }});


    Resolver resolver;
    if (cacheDns.get() == true) {
      resolver = new CachingResolver(address);
    } else {
      resolver = new Resolver(address);
    }

    // Set bootstrap options
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", true);
    bootstrap.setOption("connectTimeoutMillis", connectTimeout.get());
    bootstrap.setOption("writeBufferLowWaterMark", writeBufferLow.get());
    bootstrap.setOption("writeBufferHighWaterMark", writeBufferHigh.get());
    bootstrap.setOption("resolver", resolver);
    bootstrap.setOption("remoteAddress", resolver.resolve());

    // Connect and wait for connection ready
    final ChannelFuture result = bootstrap.connect().awaitUninterruptibly();

    // At this point we consider the client "connected"--even though the
    // connection may have failed. The channel will continue to initiate
    // reconnect attempts in the background.
    state = State.CONNECTED;

    // We'll throw an exception so users can pretend this call is synchronous
    // (and log errors as appropriate) but the client might succeed later.
    if (! result.isSuccess()) {
      throw new IOException("Connection failed", result.getCause());
    }
  }

  @Override
  public void close() {
    close(false);
  }

  public synchronized void close(boolean force) {
    if (!(force || state == State.CONNECTED)) {
      return;
    }

    try {
      timer.stop();
      channels.close().awaitUninterruptibly();
      bootstrap.releaseExternalResources();
    } finally {
      timer = null;
      bootstrap = null;
      state = State.DISCONNECTED;
    }
  }

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

    final Write write = new Write(msg, promise);
    final Semaphore limiter = writeLimiter;

    // Reserve a slot in the queue
    if (limiter.tryAcquire()) {
      for (Channel channel : channels) {
        // When the write is flushed from our local buffer, release our
        // limiter permit.
        ChannelFuture f = channel.write(write);
        f.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture f) {
            limiter.release();
          }
        });
        return promise;
      }

      // No channels
      promise.deliver(new IOException("no channels available"));
      return promise;
    }

    // Buffer's full.
    promise.deliver(
        new OverloadedException(
          "client write buffer is full: " +
          writeLimiter.availablePermits() + " / " +
          writeLimit.get() + " messages."));
    return promise;
  }

  @Override
  public Transport transport() {
    return null;
  }
}
