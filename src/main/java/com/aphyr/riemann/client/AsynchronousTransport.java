package com.aphyr.riemann.client;

import com.aphyr.riemann.Proto.Msg;
import java.io.IOException;

public interface AsynchronousTransport extends Transport {
  // Schedules a message to be sent, returns nothing.
  void aSendMessage(final Msg msg);

  // Schedules a message to be sent, returns a promise which fulfills the
  // response.
  Promise<Msg> aSendRecvMessage(final Msg msg);

  // Schedules a message to be sent, and depending on whether the transport
  // supports it, returns the response.
  Promise<Msg> aSendMaybeRecvMessage(final Msg msg);
}
