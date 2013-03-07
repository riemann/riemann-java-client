package riemann.java.client.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.aphyr.riemann.client.RiemannClient;
import com.aphyr.riemann.client.ServerError;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aphyr.riemann.Proto.Attribute;
import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.Proto.Msg;

public class TcpClientTest {
	@Test
	public void sendEventsTest() throws IOException, InterruptedException, ServerError {
    final Server server = new OkServer();
    RiemannClient client = null;
    try {
      client = RiemannClient.tcp(server.start());
      client.connect();
      for (int i = 0; i < 10; i++) {
        final Event e = Util.createEvent();
        assertEquals(true, client.sendEventsWithAck(e));
        assertEquals(e, Util.soleEvent(server.received.poll()));
      }
    } finally {
      if (client != null) {
        client.disconnect();
      }
      server.stop();
    }
  }

	@Test
	public void queryTest() throws IOException, InterruptedException, ServerError {
    final Server server = new EchoServer();
    RiemannClient client = null;
    try {
      client = RiemannClient.tcp(server.start());
      client.connect();
      for (int i = 0; i < 10; i++) {
        final List<Event> events = client.query("hi");
        assertEquals(0, events.size());
        final Msg m = server.received.poll();
        assertEquals("hi", m.getQuery().getString());
      }
    } finally {
      if (client != null) {
        client.disconnect();
      }
      server.stop();
    }
  }
}
