package io.riemann.riemann.client.netty3;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;

import io.riemann.riemann.client.*;
import org.jboss.netty.util.Timer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.*;
import org.jboss.netty.handler.codec.protobuf.*;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import io.riemann.riemann.Proto.Msg;
import org.jboss.netty.channel.socket.nio.*;

public class UdpTransport extends AbstractUdpTransport {

    public volatile ConnectionlessBootstrap bootstrap;
    public final ChannelGroup channels = new DefaultChannelGroup();
    public volatile Timer timer;

    // Shared pipeline handlers
    public static final ProtobufEncoder pbEncoder = new ProtobufEncoder();
    public final DiscardHandler discardHandler = new DiscardHandler();

    public UdpTransport(InetSocketAddress remoteAddress, InetSocketAddress localAddress) {
        super(remoteAddress, localAddress);
    }

    @Override
    protected ConnectionResult doConnect() {
        // Set up channel factory
        final ChannelFactory channelFactory = new NioDatagramChannelFactory(
            Executors.newCachedThreadPool());

        // Timer
        timer = HashedWheelTimerFactory.CreateDaemonHashedWheelTimer();

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
                }
            });

        Resolver resolver;
        Resolver localResolver = null;
        if (cacheDns.get() == true) {
            resolver = new CachingResolver(remoteAddress);
            if (localAddress != null) {
                localResolver = new CachingResolver(localAddress);
            }
        } else {
            resolver = new Resolver(remoteAddress);
            if (localAddress != null) {
                localResolver = new Resolver(localAddress);
            }
        }

        // Set bootstrap options
        bootstrap.setOption("resolver", resolver);
        bootstrap.setOption("remoteAddress", resolver.resolve());
        if (localAddress != null) {
            bootstrap.setOption("localAddress", localResolver.resolve());
        }
        bootstrap.setOption("sendBufferSize", sendBufferSize.get());

        // Connect
        final ChannelFuture result = bootstrap.connect().awaitUninterruptibly();

        return new ConnectionResult(result.isSuccess(), result.getCause());
    }

    @Override
    protected void doStop() {
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
    protected void doSend(Msg msg) {
        channels.write(msg);
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
