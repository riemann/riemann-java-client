package io.riemann.riemann.client.netty3;

import io.riemann.riemann.client.*;
import io.riemann.riemann.Proto.Msg;
import java.io.*;
import java.net.InetSocketAddress;
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

import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpTransport extends AbstractTcpTransport {
  // Logger
  public final Logger logger = LoggerFactory.getLogger(TcpTransport.class);

  // Shared pipeline handlers
  public static final ProtobufDecoder pbDecoder =
    new ProtobufDecoder(Msg.getDefaultInstance());
  public static final ProtobufEncoder pbEncoder =
    new ProtobufEncoder();
  public static final LengthFieldPrepender frameEncoder =
    new LengthFieldPrepender(4);

  // Configuration
  public final AtomicReference<SSLContext> sslContext =
    new AtomicReference<SSLContext>();

    public final ChannelGroup channels = new DefaultChannelGroup();
  public volatile Timer timer;
  public volatile ClientBootstrap bootstrap;

  public TcpTransport(final InetSocketAddress remoteAddress, final InetSocketAddress localAddress) {
    super(remoteAddress, localAddress);
  }

  @Override
  public boolean checkConnected() {
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

  protected ConnectionResult doConnect() throws IOException {
    // Set up channel factory
    final ChannelFactory channelFactory = new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool());

    // Timer
    timer = HashedWheelTimerFactory.CreateDaemonHashedWheelTimer();

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
    Resolver localResolver = null;
    if (cacheDns.get() == true) {
        resolver = new CachingResolver(remoteAddress);
      if(localAddress != null){
        localResolver = new CachingResolver(localAddress);
      }
    } else {
      resolver = new Resolver(remoteAddress);
      if( localAddress != null){
        localResolver = new Resolver(localAddress);
      }
    }

    // Set bootstrap options
    bootstrap.setOption("tcpNoDelay", true);
    bootstrap.setOption("keepAlive", true);
    bootstrap.setOption("connectTimeoutMillis", connectTimeout.get());
    bootstrap.setOption("writeBufferLowWaterMark", writeBufferLow.get());
    bootstrap.setOption("writeBufferHighWaterMark", writeBufferHigh.get());
    bootstrap.setOption("resolver", resolver);
    bootstrap.setOption("remoteAddress", resolver.resolve());
    if( localAddress != null){
      bootstrap.setOption("localAddress", localResolver.resolve());
    }

    // Connect and wait for connection ready
    final ChannelFuture result = bootstrap.connect().awaitUninterruptibly();

    return new ConnectionResult(result.isSuccess(), result.getCause());
  }

  @Override
  public void doClose() {
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
  protected boolean doSend(Write write, final Runnable completeCallback) {
    for (Channel channel : channels) {
      // When the write is flushed from our local buffer, release our
      // limiter permit.
      ChannelFuture f = channel.write(write);
      f.addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture f) {
          completeCallback.run();
        }
      });
      return true;
    }

    return false;
  }

}
