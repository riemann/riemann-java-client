package io.riemann.riemann.client;

import io.riemann.riemann.Proto.Msg;
import java.io.*;
import java.net.*;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.group.*;
import io.netty.channel.socket.nio.*;
//import io.netty.handler.codec.frame.*;
import io.netty.handler.codec.protobuf.*;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.*;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import io.netty.channel.nio.NioEventLoopGroup;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
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
  public final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE); // TODO constructor parameter
  public volatile Timer timer;
  public volatile Bootstrap bootstrap;
  public volatile EventLoopGroup workerGroup;
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
      if (ch.isActive()) {
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
    // TODO : Not exists in Netty 4 ? jdk 8 = -Djdk.tls.rejectClientInitiatedRenegotiation=true ?
    //handler.setEnableRenegotiation(false);
    //handler.setIssueHandshake(true);

    return handler;
  }

  @Override
  // Does nothing if not currently disconnected.
  public synchronized void connect() throws IOException {
    if (state != State.DISCONNECTED) {
      return;
    };
    state = State.CONNECTING;

    // Set up worker group
    // TODO : check parameters
    workerGroup = new NioEventLoopGroup(1, Executors.newCachedThreadPool());

    // Timer
    timer = HashedWheelTimerFactory.CreateDaemonHashedWheelTimer();
    final Resolver resolver;
    if (cacheDns.get() == true) {
      resolver = new CachingResolver(address);
    } else {
      resolver = new Resolver(address);
    }

    // Create bootstrap
    bootstrap = new Bootstrap();
    bootstrap.channel(NioSocketChannel.class);
    bootstrap.group(workerGroup);
    bootstrap.handler(new ChannelInitializer<NioSocketChannel>() {
      @Override
      public void initChannel(NioSocketChannel ch) throws Exception {
        final ChannelPipeline p = ch.pipeline();
        // Reconnections
        p.addLast("reconnect", new ReconnectHandler(
                bootstrap,
                timer,
                reconnectDelay,
                TimeUnit.MILLISECONDS,
                resolver));

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

      }
    });

    // Set bootstrap options
    bootstrap.remoteAddress(resolver.resolve());
    bootstrap.option(ChannelOption.TCP_NODELAY, true);
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout.get());
    bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(writeBufferLow.get(), writeBufferHigh.get()));

    // Connect and wait for connection ready
    final ChannelFuture result = bootstrap.connect().awaitUninterruptibly();

    // At this point we consider the client "connected"--even though the
    // connection may have failed. The channel will continue to initiate
    // reconnect attempts in the background.
    state = State.CONNECTED;

    // We'll throw an exception so users can pretend this call is synchronous
    // (and log errors as appropriate) but the client might succeed later.
    if (! result.isSuccess()) {
      throw new IOException("Connection failed", result.cause());
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
      workerGroup.shutdownGracefully();

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
    //channels.flush();
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
        TcpHandler handler = (TcpHandler) channel.pipeline().get("handler");

        // send the message
        ChannelFuture f = handler.sendMessage(write);
        f.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture f) {
            limiter.release();
          }
        });
        return promise;
      }

      // No channels available, release the slot.
      limiter.release();
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
