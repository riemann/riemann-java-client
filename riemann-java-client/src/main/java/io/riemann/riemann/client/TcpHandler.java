package io.riemann.riemann.client;

import java.io.*;

import io.netty.channel.*;
import io.riemann.riemann.Proto.Msg;

public class TcpHandler extends ChannelInboundHandlerAdapter {
  public final WriteQueue queue = new WriteQueue();
  private ChannelHandlerContext ctx;

  // The last error used to fulfill outstanding promises.
  public volatile IOException lastError =
    new IOException("Channel closed.");

  public final ExceptionReporter exceptionReporter;

  public TcpHandler(final ExceptionReporter exceptionReporter) {
    this.exceptionReporter = exceptionReporter;
  }

  // We receive a Write, Write the message, enqueuing
  // the corresponding promise when the write completes.
  public ChannelFuture sendMessage(Write write) {
    final Msg message = write.message;
    final Promise promise = write.promise;
    // write the message to the channelHandlerContext
    final ChannelFuture f = ctx.write(message);
    // When the message event is written...
    f.addListener(new ChannelFutureListener() {
      // Enqueue the corresponding promise.
      public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
          queue.put(promise);
        } else if (future.cause() != null) {
          promise.deliver(
                  new IOException("Write failed.", future.cause()));
        } else {
          promise.deliver(new IOException("Write failed."));
        }
      }
    });
    ctx.flush();
    return f;
  }

  // When messages are received, deliver them to the next queued promise.
  // Equivalent to messageReceived in netty 3
  @Override
  public void channelRead(ChannelHandlerContext c, Object msg) {
    Msg message = (Msg) msg;
    queue.take().deliver(message);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    this.ctx = ctx;
  }

  // Log exceptions and close.
  @Override
  public void exceptionCaught(ChannelHandlerContext c, Throwable cause) {
    try {
      exceptionReporter.reportException(cause);
    } catch (final Exception ee) {
      // Oh well
    }

    queue.close(cause);
    c.channel().close();
  }
}
