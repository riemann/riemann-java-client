package com.aphyr.riemann.client;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CopyOnWriteArrayList;
import java.net.*;
import java.io.*;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.*;
import org.jboss.netty.handler.codec.oneone.*;
import org.jboss.netty.handler.codec.protobuf.*;
import org.jboss.netty.handler.codec.frame.*;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import com.aphyr.riemann.Proto.Msg;
import org.jboss.netty.channel.socket.nio.*;
import java.util.concurrent.atomic.*;

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
  public volatile ConnectionlessBootstrap bootstrap;
  public final ChannelGroup channels = new DefaultChannelGroup();

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

    // Set up channel factory
    final ChannelFactory channelFactory = new NioDatagramChannelFactory(
        Executors.newCachedThreadPool());

    // Timer
    timer = new HashedWheelTimer();

    // Create bootstrap
    bootstrap = new ConnectionlessBootstrap(channelFactory);

    // Set up pipeline factory.
    bootstrap.setPipelineFactory(
        new ChannelPipelineFactory() {
          public ChannelPipeline getPipeline() {
            final ChannelPipeline p = Channels.pipeline();

            p.addLast("reconnect", new ReconnectHandler(
                bootstrap,
                timer,
                reconnectDelay,
                TimeUnit.MILLISECONDS));
            p.addLast("protobuf-encoder", pbEncoder);
            p.addLast("channelgroups", new ChannelGroupHandler(channels));
            p.addLast("discard", discardHandler);
            
            return p;
          }});

    Resolver resolver;
    if (cacheDns.get() == true) {
      resolver = new CachingResolver(address);
    } else {
      resolver = new Resolver(address);
    }

    // Set bootstrap options
    bootstrap.setOption("resolver", resolver);
    bootstrap.setOption("remoteAddress", resolver.resolve());
    bootstrap.setOption("sendBufferSize", sendBufferSize.get());

    // Connect
    final ChannelFuture result = bootstrap.connect().awaitUninterruptibly();
    
    // Check for errors.
    if (! result.isSuccess()) {
      close(true);
      throw new IOException("Connection failed", result.getCause());
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
          bootstrap.releaseExternalResources();
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

  public class DiscardHandler extends SimpleChannelHandler {
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
      ctx.getChannel().setReadable(false);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      try {
        exceptionReporter.reportException(e.getCause());
      } catch (final Exception ee) {
        // Oh well
      } finally {
        try {
          ctx.getChannel().close();
        } catch (final Exception ee) {
          exceptionReporter.reportException(ee);
        }
      }
    }
  }
}
