package com.aphyr.riemann.client;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CopyOnWriteArrayList;
import java.net.*;
import java.io.*;
import org.jboss.netty.util.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.*;
import org.jboss.netty.handler.codec.oneone.*;
import org.jboss.netty.handler.codec.protobuf.*;
import org.jboss.netty.handler.codec.frame.*;
import org.jboss.netty.bootstrap.Bootstrap;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.timeout.*;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import com.aphyr.riemann.Proto.Msg;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.LinkedBlockingQueue;
import java.nio.channels.*;

public class ReconnectHandler extends SimpleChannelUpstreamHandler {
  final Bootstrap bootstrap;
  public final Timer timer;
  public long startTime = -1;
  public final AtomicLong delay;
  public final TimeUnit unit;

  public ReconnectHandler(ClientBootstrap bootstrap, Timer timer, AtomicLong delay, TimeUnit unit) {
    this.bootstrap = bootstrap;
    this.timer = timer;
    this.delay = delay;
    this.unit = unit;
  }
  
  public ReconnectHandler(ConnectionlessBootstrap bootstrap, Timer timer, AtomicLong delay, TimeUnit unit) {
    this.bootstrap = bootstrap;
    this.timer = timer;
    this.delay = delay;
    this.unit = unit;
  }

  InetSocketAddress getRemoteAddress() {
    Resolver resolver = (Resolver) bootstrap.getOption("resolver");
    return resolver.resolve();
  }

  @Override
  public void channelDisconnected(ChannelHandlerContext c, ChannelStateEvent e) throws Exception {
    // Go ahead and close. I don't know why Netty doesn't close disconnected
    // TCP sockets, but it seems not to.
    e.getChannel().close();
    super.channelDisconnected(c, e);
  }

  @Override
  public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    try {
      timer.newTimeout(new TimerTask() {
        public void run(Timeout timeout) throws Exception {
          if (bootstrap instanceof ClientBootstrap) {
            ClientBootstrap b = (ClientBootstrap) bootstrap;
            b.setOption("remoteAddress", getRemoteAddress());
            b.connect();
          } else if (bootstrap instanceof ConnectionlessBootstrap) {
            ConnectionlessBootstrap b = (ConnectionlessBootstrap) bootstrap;
            b.setOption("remoteAddress", getRemoteAddress());
            b.connect();
          }
        }
      }, delay.get(), unit);
    } catch (java.lang.IllegalStateException ex) {
      // The timer must have been stopped.
    }
    super.channelClosed(ctx, e);
  }

  @Override
  public void channelConnected(ChannelHandlerContext c, ChannelStateEvent e) throws Exception {
    if (startTime < 0) {
      startTime = System.currentTimeMillis();
    }
    super.channelConnected(c, e);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext c, ExceptionEvent e) {
    final Throwable cause = e.getCause();

    if (cause instanceof ConnectException) {
      startTime = -1;
    } else if (cause instanceof ReadTimeoutException) {
      // The connection was OK but there was no traffic for the last period.
    } else {
     c.sendUpstream(e);
    }
    c.getChannel().close();
  }
}
