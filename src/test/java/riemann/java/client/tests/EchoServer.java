package riemann.java.client.tests;

import com.aphyr.riemann.Proto.Msg;

public class EchoServer extends Server {
  final long delay;

  public EchoServer() {
    this(0);
  }

  public EchoServer(final long delay) {
    this.delay = delay;
  }

  public Msg handle(final Msg m) {
    if (0 < delay) {
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
      }
    }
    if (0 < m.getEventsCount()) {
      System.out.println("Server: " + m.getEvents(0).getMetricSint64());
    }
    return m;
  }
}
