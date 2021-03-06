package io.riemann.riemann.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.timeout.ReadTimeoutException;
import java.net.ConnectException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ReconnectHandler extends ChannelInboundHandlerAdapter {
  private final Bootstrap bootstrap;
  private final ChannelGroup channels;
  public long startTime = -1;
  public final AtomicLong delay;
  public final TimeUnit unit;

  public ReconnectHandler(Bootstrap bootstrap, ChannelGroup channels, AtomicLong delay, TimeUnit unit) {
    this.bootstrap = bootstrap;
    this.channels = channels;
    this.delay = delay;
    this.unit = unit;
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    try {
      ctx.executor().schedule(new Runnable() {
        @Override
        public void run() {
          ChannelFuture channelFuture = bootstrap.connect();
          channels.add(channelFuture.channel());
        }
      }, delay.get(), unit);
    } catch (java.lang.IllegalStateException ex) {
      // The executor must have been stopped.
    }
    super.channelInactive(ctx);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    if (startTime < 0) {
      startTime = System.currentTimeMillis();
    }
    super.channelActive(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
    final Throwable cause = e.getCause();

    if (cause instanceof ConnectException) {
      startTime = -1;
    } else if (cause instanceof ReadTimeoutException) {
      // The connection was OK but there was no traffic for the last period.
    } else {
      ctx.write(e);
    }
    ctx.channel().close();
  }
}
