package io.riemann.riemann.client;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;

/**
 * Keep track of open channel(s)
 *
 */
public class ChannelGroupHandler extends SimpleChannelUpstreamHandler {

  // A channel group so we can keep track of open channels.
  public final ChannelGroup channelGroup;

  public ChannelGroupHandler(final ChannelGroup channelGroup) {
    this.channelGroup = channelGroup;
  }

  // When we open, add our channel to the channel group.
  @Override
  public void channelOpen(ChannelHandlerContext c, ChannelStateEvent e) throws Exception {
    channelGroup.add(e.getChannel());
    super.channelOpen(c, e);
  }

  @Override
  public void channelClosed(ChannelHandlerContext c, ChannelStateEvent e) throws Exception {
    // Remove us from the channel group
    channelGroup.remove(e.getChannel());
    super.channelClosed(c, e);
  }

}
