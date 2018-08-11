package io.riemann.riemann.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.riemann.riemann.Proto.Msg;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
  public volatile Bootstrap bootstrap;
  public final EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
  public final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  // Configuration
  public final AtomicLong reconnectDelay = new AtomicLong(5000);
  public final AtomicLong connectTimeout = new AtomicLong(5000);
  // Changes to this value are applied only on reconnect.
  public final AtomicInteger sendBufferSize = new AtomicInteger(16384);
  public final AtomicBoolean autoFlush = new AtomicBoolean(true);

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

  public UdpTransport(final InetSocketAddress remoteAddress) {
    this.remoteAddress = remoteAddress;
    this.localAddress = null;
  }

  public UdpTransport(final InetSocketAddress remoteAddress,final InetSocketAddress localAddress) {
    this.remoteAddress = remoteAddress;
    this.localAddress = localAddress;
  }

  public UdpTransport(final String host, final int port) throws IOException {
    this(new InetSocketAddress(host, port));
  }

  public UdpTransport(final String remoteHost, final int remotePort, final String localHost, final int localPort) throws IOException {
    this(new InetSocketAddress(remoteHost, remotePort),new InetSocketAddress(localHost, localPort) );
  }

  public UdpTransport(final String remoteHost) throws IOException {
    this(remoteHost, DEFAULT_PORT);
  }

  public UdpTransport(final String remoteHost, final String localHost) throws IOException {
    this(remoteHost, DEFAULT_PORT, localHost, 0);
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

    // Create bootstrap
    bootstrap = new Bootstrap().group(eventLoopGroup)
                  .channel(NioDatagramChannel.class);

    // Set up pipeline factory.
    bootstrap.handler(
      new ChannelInitializer() {
        @Override
        protected void initChannel(Channel ch) throws Exception {
          ChannelPipeline p = ch.pipeline();
          p.addLast("reconnect", new ReconnectHandler(
            bootstrap,
            reconnectDelay,
            TimeUnit.MILLISECONDS));
          p.addLast("protobuf-encoder", pbEncoder);
          p.addLast("channelgroups", new ChannelGroupHandler(channels));
          p.addLast("discard", discardHandler);
        }
      }
    );

    // Set bootstrap options
    bootstrap.remoteAddress(remoteAddress);
    bootstrap.localAddress(localAddress);
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
    // Close channel
    try {
      channels.close().awaitUninterruptibly();
    } finally {

      // Stop bootstrap
      try {
        eventLoopGroup.shutdownGracefully();
      } finally {
        bootstrap = null;
        state = State.DISCONNECTED;
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
    channels.flush();
  }

  @Override
  public Msg sendMessage(final Msg msg) {
    if (autoFlush.get()) {
      channels.writeAndFlush(msg);
    } else {
      channels.write(msg);
    }

    return null;
  }

  @Override
  public Transport transport() {
    return null;
  }

  public class DiscardHandler extends ChannelInboundHandlerAdapter {

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
