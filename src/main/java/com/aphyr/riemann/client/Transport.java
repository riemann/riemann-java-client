package com.aphyr.riemann.client;

import java.io.IOException;

// A common transport interface.
public interface Transport {
  boolean isConnected();
  void connect() throws IOException;
  void disconnect() throws IOException;
  void reconnect() throws IOException;
  void flush() throws IOException;
}
