package riemann.java.client.tests;

import io.riemann.riemann.Proto.Msg;

public class OkServer extends Server {
  public Msg handle(final Msg m) {
    return Msg.newBuilder().build();
  }
}
