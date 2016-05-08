package io.riemann.riemann.client;

// Validates the integrity of messages, throwing ServerError if one is not OK.
// Returns the message otherwise.

import io.riemann.riemann.Proto.Msg;

public class MsgValidator implements Fn2<Msg, Msg> {
  public Msg call(final Msg message) throws ServerError {
    if (message.hasOk() && !message.getOk()) {
      throw new ServerError(message.getError());
    }
    return message;
  }
}
