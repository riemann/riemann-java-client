package io.riemann.riemann.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;

/**
 * Keep track of open channel(s)
 *
 */
public class ChannelGroupHandler extends ChannelInboundHandlerAdapter {

  // A channel group so we can keep track of open channels.
  private final ChannelGroup channelGroup;

  public ChannelGroupHandler(final ChannelGroup channelGroup) {
    this.channelGroup = channelGroup;
  }

  // When we open, add our channel to the channel group.
  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    channelGroup.add(ctx.channel());
    super.channelActive(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    // Netty 4 - removing inactive channel from the ChannelGroup is already handled by Netty
    // no need for anything explicit here
    super.channelInactive(ctx);
  }
}
