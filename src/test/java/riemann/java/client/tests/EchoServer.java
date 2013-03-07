package riemann.java.client.tests;

import com.aphyr.riemann.Proto.Msg;

public class EchoServer extends Server {
  public Msg handle(final Msg m) {
    return m;
  }
}
