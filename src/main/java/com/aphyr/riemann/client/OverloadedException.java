package com.aphyr.riemann.client;

// Thrown when a client is unable to handle additional requests; for example,
// because of high network latencies
public class OverloadedException extends java.io.IOException {
  public OverloadedException(final String msg) {
    super(msg);
  }
}
