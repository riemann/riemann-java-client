package com.aphyr.riemann.client;

import java.io.IOException;
import java.lang.AutoCloseable;

// A common transport interface.
public interface Transport extends AutoCloseable {
  boolean isConnected();
  void connect() throws IOException;

  void reconnect() throws IOException;

  void flush() throws IOException;

  // Our close should never throw.
  void close();

  Transport transport();
}
