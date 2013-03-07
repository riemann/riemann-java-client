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
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import com.aphyr.riemann.Proto.Msg;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.LinkedBlockingQueue;

public class TcpHandler extends SimpleChannelHandler {
  public final LinkedBlockingQueue<Promise<Msg>> queue =
    new LinkedBlockingQueue<Promise<Msg>>();

  // The channel associated with this handler.
  public volatile Channel channel;

  // The last error used to fulfill outstanding promises.
  public volatile IOException lastError =
    new IOException("Channel closed.");

  // A channel group so we can keep track of open channels.
  public final ChannelGroup channelGroup;

  public TcpHandler(final ChannelGroup channelGroup) {
    this.channelGroup = channelGroup;
  }

  // When we open, add our channel to the channel group.
  @Override
  public void channelOpen(ChannelHandlerContext c, ChannelStateEvent e) {
    channelGroup.add(e.getChannel());
  }

  // When we connect, save the channel so we can write to it.
  @Override
  public void channelConnected(ChannelHandlerContext c, ChannelStateEvent e) {
    channel = e.getChannel();
  }

  @Override
  public void channelClosed(ChannelHandlerContext c, ChannelStateEvent e) {
    // Kill the channel, so subsequent writes can fail quickly.
    channel = null;

    // Remove us from the channel group too
    channelGroup.remove(e.getChannel());

    // Another thread might still be calling write(), and have a copy of the
    // channel. At some point in the future, it's going to call channel.write()
    // and the channel will tell it the write failed because it's *closed*.
    // This means there will be no additional enqueued futures, and since the
    // channel is closed, no additional messages will be received. We are now
    // the sole owners of the message queue. Right? I hope?
    
    // Deliver exceptions to any remaining queued promises.
    final IOException ex = new IOException("Connection closed.");
    Promise<Msg> promise;
    while ((promise = queue.poll()) != null) {
      promise.deliver(ex);
    }
  }

  // We receive a Write, and pass the Write's message downstream, enqueuing the
  // corresponding promise when the write completes.
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
    ctx.sendDownstream(me);
    // Channels.write(ctx, me.getFuture(), message).
  }

  // When messages are received, deliver them to the next queued promise.
  @Override
  public void messageReceived(ChannelHandlerContext c, MessageEvent e) {
    Msg message = (Msg) e.getMessage();
    queue.poll().deliver(message);
  }

  // Log exceptions and close.
  @Override
  public void exceptionCaught(ChannelHandlerContext c, ExceptionEvent e) {
    e.getChannel().close();
  }
}
