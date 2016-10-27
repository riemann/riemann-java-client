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
  public final ChannelGroup channelGroup;

  public ChannelGroupHandler(final ChannelGroup channelGroup) {
    this.channelGroup = channelGroup;
  }

  // When we open, add our channel to the channel group.
  @Override
  public void channelActive(ChannelHandlerContext c) throws Exception {
    channelGroup.add(c.channel());
    super.channelActive(c);
  }

  @Override
  public void channelInactive(ChannelHandlerContext c) throws Exception {
    channelGroup.remove(c.channel());
    super.channelInactive(c);
  }

}
