package io.riemann.riemann.client;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.net.*;
import java.io.*;
import io.netty.channel.socket.DatagramChannel;
import io.netty.util.HashedWheelTimer;
import io.netty.buffer.ByteBuf;
import io.netty.util.Timer;
import io.netty.channel.*;
import io.netty.channel.group.*;
import io.netty.handler.codec.protobuf.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.nio.NioEventLoopGroup;
import io.riemann.riemann.Proto.Msg;
import io.netty.channel.socket.nio.*;
import java.util.concurrent.atomic.*;
import io.netty.util.concurrent.GlobalEventExecutor;

public class UdpTransport implements SynchronousTransport {
  // For writes we don't care about
  public static final Promise<Msg> blackhole = 
    new Promise<Msg>();

  // Shared pipeline handlers
  public static final ProtobufEncoder pbEncoder = new ProtobufEncoder();
  public final DiscardHandler discardHandler = new DiscardHandler();

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
  public volatile Timer timer;
  public volatile Bootstrap bootstrap;
  public volatile EventLoopGroup workerGroup;
  public final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE); // TODO constructor parameter

  // Configuration
  public final AtomicLong reconnectDelay = new AtomicLong(5000);
  public final AtomicLong connectTimeout = new AtomicLong(5000);
  // Changes to this value are applied only on reconnect.
  public final AtomicInteger sendBufferSize = new AtomicInteger(16384);
  public final AtomicBoolean cacheDns = new AtomicBoolean(true);
  public final InetSocketAddress address;

  public volatile ExceptionReporter exceptionReporter = new ExceptionReporter() {
    @Override
    public void reportException(final Throwable t) {
    t.printStackTrace();
    }
  };

  public void setExceptionReporter(final ExceptionReporter exceptionReporter) {
    this.exceptionReporter = exceptionReporter;
  }

  public UdpTransport(final InetSocketAddress address) {
    this.address = address;
  }

  public UdpTransport(final String host, final int port) throws IOException {
    this(new InetSocketAddress(host, port));
  }

  public UdpTransport(final String host) throws IOException {
    this(host, DEFAULT_PORT);
  }

  public UdpTransport(final int port) throws IOException {
    this(InetAddress.getLocalHost().getHostAddress(), port);
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
    };
    state = State.CONNECTING;

    // create the workergroup
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
    bootstrap.group(workerGroup);
    bootstrap.channel(NioDatagramChannel.class);
    bootstrap.handler(new ChannelInitializer<DatagramChannel>() {
      @Override
      public void initChannel(DatagramChannel ch) throws Exception {
        final ChannelPipeline p = ch.pipeline();
        p.addLast("reconnect", new ReconnectHandler(
                bootstrap,
                timer,
                reconnectDelay,
                TimeUnit.MILLISECONDS,
                resolver));
        p.addLast("protobuf-encoder", pbEncoder);
        p.addLast("channelgroups", new ChannelGroupHandler(channels));
        p.addLast("discard", discardHandler);

      }
    });

    // Set bootstrap options
    bootstrap.remoteAddress(resolver.resolve());
    //bootstrap.option("resolver", resolver);
    bootstrap.option(ChannelOption.SO_SNDBUF, sendBufferSize.get());

    // Connect
    final ChannelFuture result = bootstrap.connect().awaitUninterruptibly();

    // Check for errors.
    if (! result.isSuccess()) {
      close(true);
      throw new IOException("Connection failed", result.cause());
    }

    // Done
    state = State.CONNECTED;
  }

  @Override
  public void close() {
    close(false);
  }

  public synchronized void close(boolean force) {
    if (!(force || state == State.CONNECTED)) {
      return;
    }

    // Stop timer
    try {
      if (timer != null) {
        timer.stop();
      }
    } finally {
      timer = null;

      // Close channel
      try {
        channels.close().awaitUninterruptibly();
      } finally {

        // Stop bootstrap
        try {
          workerGroup.shutdownGracefully();
        } finally {
          bootstrap = null;
          state = State.DISCONNECTED;
        }
      }
    }
  }

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
    channels.write(msg);
    return null;
  }

  @Override
  public Transport transport() {
    return null;
  }

  public class DiscardHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      ((ByteBuf) msg).release();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      ctx.channel().config().setAutoRead(false);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      try {
        exceptionReporter.reportException(cause);
      } catch (final Exception ee) {
        // Oh well
      } finally {
        try {
          ctx.channel().close();
        } catch (final Exception ee) {
          exceptionReporter.reportException(ee);
        }
      }
    }
  }
}
