package io.riemann.riemann.client;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.net.*;

import io.netty.util.*;
import io.netty.channel.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.handler.timeout.*;
import io.riemann.riemann.Proto.Msg;
import java.util.concurrent.atomic.AtomicLong;

public class ReconnectHandler extends ChannelInboundHandlerAdapter {
  final Bootstrap bootstrap;
  public final Timer timer;
  public long startTime = -1;
  public final AtomicLong delay;
  public final TimeUnit unit;
  public final Resolver resolver;

  public ReconnectHandler(Bootstrap bootstrap, Timer timer, AtomicLong delay, TimeUnit unit, Resolver resolver) {
    this.bootstrap = bootstrap;
    this.timer = timer;
    this.delay = delay;
    this.unit = unit;
    this.resolver = resolver;
  }

  InetSocketAddress getRemoteAddress() {
    return resolver.resolve();
  }

  @Override
  public void channelInactive(ChannelHandlerContext c) {
    c.channel().close();
    try {
      timer.newTimeout(new TimerTask() {
        public void run(Timeout timeout) throws Exception {
          bootstrap.remoteAddress(getRemoteAddress());
          bootstrap.connect();

        }
      }, delay.get(), unit);
    } catch (java.lang.IllegalStateException ex) {
      // The timer must have been stopped.
    }
    // super.channelInactive(c); TODO
  }


  @Override
  public void channelActive(ChannelHandlerContext c) throws Exception {
    if (startTime < 0) {
      startTime = System.currentTimeMillis();
    }
    super.channelActive(c);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext c, Throwable cause) {
    if (cause instanceof ConnectException) {
      startTime = -1;
    } else if (cause instanceof ReadTimeoutException) {
      // The connection was OK but there was no traffic for the last period.
    } else {
     c.fireChannelRead(c); // TODO c in parameter ?
    }
    c.channel().close();
  }
}
