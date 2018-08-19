package io.riemann.riemann.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.CombinedChannelDuplexHandler;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.riemann.riemann.Proto.Msg;
import java.io.IOException;

public class TcpHandler
    extends CombinedChannelDuplexHandler<TcpHandler.Inbound, TcpHandler.Outbound> {

  public final WriteQueue queue = new WriteQueue();
  public final ExceptionReporter exceptionReporter;
  // The last error used to fulfill outstanding promises.
  public volatile IOException lastError =
    new IOException("Channel closed.");

  public TcpHandler(final ExceptionReporter exceptionReporter) {
    this.exceptionReporter = exceptionReporter;
    init(new Inbound(), new Outbound());
  }

  public class Inbound extends SimpleChannelInboundHandler<Msg> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Msg msg) {
      // When messages are received, deliver them to the next queued promise.
      queue.take().deliver(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      // Log exceptions and close.
      try {
        exceptionReporter.reportException(cause);
      } catch (final Exception ee) {
        // Oh well
      }

      queue.close(cause);
      ctx.channel().close();
      super.exceptionCaught(ctx, cause);
    }
  }

  public class Outbound extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, final ChannelPromise channelPromise)
        throws Exception {
      // Destructure the write
      final Write write = (Write) msg;
      final Promise<Msg> promise = write.promise;

      channelPromise.addListener(
          new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
              if (future.isSuccess()) {
                queue.put(promise);
              } else if (future.cause() != null) {
                promise.deliver(new IOException("Write failed.", future.cause()));
              } else {
                promise.deliver(new IOException("Write failed."));
              }
            }
          });
      super.write(ctx, ((Write) msg).message, channelPromise);
    }
  }
}
