package io.riemann.riemann.client;

import io.riemann.riemann.Proto.Msg;

public interface AsynchronousTransport extends Transport {
  // Schedules a message to be sent, returns a promise which fulfills the
  // response. There are *no* guarantees that an asynchronous message will be
  // delivered in order, or at all; you *must* dereference the returned promise.
  IPromise<Msg> sendMessage(final Msg msg);
}
