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
  public static final DiscardHandler discardHandler = new DiscardHandler();

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
  public volatile Channel channel = null;
  public volatile ConnectionlessBootstrap bootstrap;

  // Configuration
  public final AtomicLong connectTimeout = new AtomicLong(5000);
  // Changes to this value are applied only on reconnect.
  public final AtomicInteger sendBufferSize = new AtomicInteger(16384);
  public final InetSocketAddress address;

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

    // Create bootstrap
    bootstrap = new ConnectionlessBootstrap(channelFactory);

    // Set up pipeline factory.
    bootstrap.setPipelineFactory(
        new ChannelPipelineFactory() {
          public ChannelPipeline getPipeline() {
            final ChannelPipeline p = Channels.pipeline();

            p.addLast("protobuf-encoder", pbEncoder);
            p.addLast("discard", discardHandler);
            
            return p;
          }});

    // Set bootstrap options
    bootstrap.setOption("remoteAddress", address);
    bootstrap.setOption("sendBufferSize", sendBufferSize.get());

    // Connect
    final ChannelFuture result = bootstrap.connect().awaitUninterruptibly();
    
    // Check for errors.
    if (! result.isSuccess()) {
      disconnect(true);
      throw new IOException("Connection failed", result.getCause());
    }
    
    // Set up channel
    channel = result.getChannel();
    channel.setReadable(false);

    // Done
    state = State.CONNECTED;
  }

  @Override
  public void disconnect() throws IOException {
    disconnect(false);
  }

  public synchronized void disconnect(boolean force) throws IOException {
    if (!(force || state == State.CONNECTED)) {
      return;
    }

    try {
      channel.close().awaitUninterruptibly();
      bootstrap.releaseExternalResources();
    } finally {
      bootstrap = null;
      channel = null;
      state = State.DISCONNECTED;
    }
  }

  @Override
  public void reconnect() throws IOException {
    disconnect();
    connect();
  }

  // An Noop
  @Override
  public void flush() throws IOException {
  }

  @Override
  public Msg sendRecvMessage(final Msg msg) {
    throw new UnsupportedOperationException("UDP transport doesn't support receiving messages");
  }

  @Override
  public Msg sendMaybeRecvMessage(final Msg msg) {
    channel.write(msg);
    return null;
  }




  public static class DiscardHandler extends SimpleChannelHandler {
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      e.getCause().printStackTrace();

      Channel ch = e.getChannel();
      ch.close();
    }
  }
}
