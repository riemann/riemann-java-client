package com.aphyr.riemann.client;

import com.aphyr.riemann.Proto.Msg;

public class Write {
  public final Msg message;
  public final Promise<Msg> promise;

  public Write(final Msg message, final Promise<Msg> promise) {
    this.message = message;
    this.promise = promise;
  }
}
