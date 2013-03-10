package com.aphyr.riemann.client;

import com.aphyr.riemann.Proto.Msg;
import java.io.IOException;

public interface SynchronousTransport extends Transport {
  // Sends a message and waits for its response, if possible. If the underlying
  // transport doesn't support receiving a response, returns null.
  public Msg sendMaybeRecvMessage(final Msg msg) throws IOException;
  
  // Sends a message and waits for its response.
  public Msg sendRecvMessage(final Msg msg) throws IOException;
}
