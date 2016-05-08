package io.riemann.riemann.client;

import io.riemann.riemann.Proto.Msg;
import java.io.IOException;

public interface SynchronousTransport extends Transport {
  // Sends a message and waits for its response, if possible. If the underlying
  // transport doesn't support receiving a response, returns null.
  public Msg sendMessage(final Msg msg) throws IOException;
}
