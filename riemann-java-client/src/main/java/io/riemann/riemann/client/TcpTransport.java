package io.riemann.riemann.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.riemann.riemann.Proto.Msg;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
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
  public final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1);
  public final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  public volatile Bootstrap bootstrap;
  public volatile Semaphore writeLimiter = new Semaphore(8192);

  // Configuration
  public final AtomicBoolean autoFlush      = new AtomicBoolean(true);
  public final AtomicInteger writeLimit     = new AtomicInteger(8192);
  public final AtomicLong    reconnectDelay = new AtomicLong(5000);
  public final AtomicInteger connectTimeout = new AtomicInteger(5000);
  public final AtomicInteger writeTimeout   = new AtomicInteger(5000);
  public final AtomicInteger writeBufferHigh = new AtomicInteger(1024 * 64);
  public final AtomicInteger writeBufferLow  = new AtomicInteger(1024 * 8);

  public final InetSocketAddress remoteAddress;
  public final InetSocketAddress localAddress;
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

  public TcpTransport(final InetSocketAddress remoteAddress) {
    this.remoteAddress = remoteAddress;
    this.localAddress = null;
  }

  public TcpTransport(final InetSocketAddress remoteAddress, final InetSocketAddress localAddress) {
    this.remoteAddress = remoteAddress;
    this.localAddress = localAddress;
  }

  public TcpTransport(final String remoteHost, final int remotePort) throws IOException {
    this(InetSocketAddress.createUnresolved(remoteHost, remotePort));
  }

  public TcpTransport(
      final String remoteHost, final int remotePort, final String localHost, final int localPort)
      throws IOException {
    this(
        InetSocketAddress.createUnresolved(remoteHost, remotePort), InetSocketAddress.createUnresolved(localHost, localPort));
  }

  public TcpTransport(final String remoteHost) throws IOException {
    this(remoteHost, DEFAULT_PORT);
  }

  public TcpTransport(final String remoteHost, final String localHost) throws IOException {
    this(remoteHost, DEFAULT_PORT, localHost, 0);
  }

  public TcpTransport(final int remotePort) throws IOException {
    this(InetAddress.getLocalHost().getHostAddress(), remotePort);
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
      if (ch.isOpen()) {
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

    // to disable tls renegotiation see:
    // https://stackoverflow.com/questions/31418644/is-it-possible-to-disable-tls-renegotiation-in-netty-4

    return handler;
  }

  @Override
  // Does nothing if not currently disconnected.
  public synchronized void connect() throws IOException {
    if (state != State.DISCONNECTED) {
      return;
    }
    state = State.CONNECTING;

    // Create bootstrap
    bootstrap = new Bootstrap().group(eventLoopGroup)
      .localAddress(localAddress)
      .remoteAddress(remoteAddress)
      .channel(NioSocketChannel.class)
      .handler(
        new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel channel) {
            ChannelPipeline p = channel.pipeline();
            // Reconnections
            p.addLast(
              "reconnect",
              new ReconnectHandler(bootstrap, reconnectDelay, TimeUnit.MILLISECONDS));

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
          }});

    // Set bootstrap options
    bootstrap.option(ChannelOption.TCP_NODELAY, true);
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout.get());
    bootstrap.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, writeBufferLow.get());
    bootstrap.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, writeBufferHigh.get());
    bootstrap.localAddress(localAddress);
    bootstrap.remoteAddress(remoteAddress);

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
      channels.close().awaitUninterruptibly();
      eventLoopGroup.shutdownGracefully().awaitUninterruptibly();
    } finally {
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
    channels.flush();
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
        ChannelFuture f;
        if (autoFlush.get()) {
          f = channel.writeAndFlush(write);
        } else {
          f = channel.write(write);
        }
        f.addListener(
            new ChannelFutureListener() {
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
            "client write buffer is full: "
                + writeLimiter.availablePermits()
                + " / "
                + writeLimit.get()
                + " messages."));
    return promise;
  }

  @Override
  public Transport transport() {
    return null;
  }
}
