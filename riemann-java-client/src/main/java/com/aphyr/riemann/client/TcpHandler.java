package com.aphyr.riemann.client;

import java.io.*;

import org.jboss.netty.channel.*;
import com.aphyr.riemann.Proto.Msg;

public class TcpHandler extends SimpleChannelHandler {
  public final WriteQueue queue = new WriteQueue();

  // The last error used to fulfill outstanding promises.
  public volatile IOException lastError =
    new IOException("Channel closed.");

  public final ExceptionReporter exceptionReporter;

  public TcpHandler(final ExceptionReporter exceptionReporter) {
    this.exceptionReporter = exceptionReporter;
  }

  // We receive a Write, and pass the Write's message downstream, enqueuing
  // the corresponding promise when the write completes.
  public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e)
    throws Exception {

    // Pass through anything that isn't a message event
    if (!(e instanceof MessageEvent)) {
      ctx.sendDownstream(e);
      return;
    }

    // Bounce back anything that isn't a Write.
    final MessageEvent me = (MessageEvent) e;
    if (!(me.getMessage() instanceof Write)) {
     ctx.sendUpstream(me);
      return;
    }

    // Destructure the write
    final Write write = (Write) me.getMessage();
    final Msg message = write.message;
    final Promise promise = write.promise;

    // When the message event is written...
    me.getFuture().addListener(new ChannelFutureListener() {
      // Enqueue the corresponding promise.
      public void operationComplete(ChannelFuture future) throws Exception {
        if (future.isSuccess()) {
          queue.put(promise);
        } else if (future.getCause() != null) {
          promise.deliver(
            new IOException("Write failed.", future.getCause()));
        } else {
          promise.deliver(new IOException("Write failed."));
        }
      }
    });

    // Send the message event downstream
    Channels.write(ctx, me.getFuture(), message);
  }

  // When messages are received, deliver them to the next queued promise.
  @Override
  public void messageReceived(ChannelHandlerContext c, MessageEvent e) {
    Msg message = (Msg) e.getMessage();
    queue.take().deliver(message);
  }

  // Log exceptions and close.
  @Override
  public void exceptionCaught(ChannelHandlerContext c, ExceptionEvent e) {
    try {
      exceptionReporter.reportException(e.getCause());
    } catch (final Exception ee) {
      // Oh well
    }

    queue.close(e.getCause());
    e.getChannel().close();
  }
}
