package riemann.java.client.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.aphyr.riemann.client.RiemannClient;
import com.aphyr.riemann.client.TcpTransport;
import com.aphyr.riemann.client.IRiemannClient;
import com.aphyr.riemann.client.ServerError;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aphyr.riemann.client.IPromise;

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
        final Msg rsp = client.sendEvent(e).deref();
        assertEquals(true, !rsp.hasOk() || rsp.getOk());
        assertEquals(e, Util.soleEvent(server.received.poll()));
      }
    } finally {
      if (client != null) {
        client.close();
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
        final List<Event> events = client.query("hi").deref();
        assertEquals(0, events.size());
        final Msg m = server.received.poll();
        assertEquals("hi", m.getQuery().getString());
      }
    } finally {
      if (client != null) {
        client.close();
      }
      server.stop();
    }
  }

  @Test
  public void backpressureTest() throws IOException {
    // Milliseconds
    final long delay = 2; // Server time to process a message
    final long fast = 1;                         // Async latencies
    final double slow = ((double) delay) * 0.95; // Backpressure latencies

    final Server server = new EchoServer(delay);
    IRiemannClient client = null;

    try {
      client = RiemannClient.tcp(server.start());
      ((TcpTransport) client.transport()).setWriteBufferLimit(1);
      client.connect();

      // How many backpressured writes do we need to see?
      final long requiredSlowWrites = 500000;

      // Bail out EVENTUALLY
      final int maxWrites = 1000000;

      long t0;
      long latency;
      long slowWrites = 0;
      IPromise<Msg> res = null;

      for (int i = 0; i < maxWrites; i++) {
        // Measure the time it takes to call .send()
        t0 = System.nanoTime();
        res = client.event().service("slow").metric(i).send();
        latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        System.out.println(i + " - " + latency);

        if (latency < fast) {
          // We're not in backpressure mode yet.
//          assertTrue(0 == slowWrites);
        } else if (latency < slow) {
          // Probably transitioning to slow mode
        } else {
          // We're in slow mode
          slowWrites++;
          if (requiredSlowWrites <= slowWrites) {
            // Test complete!
            break;
          }
        }
      }

      if (res != null) {
        res.deref();
      }
    } finally {
      if (client != null) {
        client.close();
      }
      server.stop();
    }
  }
}
