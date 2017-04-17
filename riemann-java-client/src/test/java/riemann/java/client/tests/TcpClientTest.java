
package riemann.java.client.tests;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.*;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;

import io.riemann.riemann.client.RiemannClient;
import io.riemann.riemann.client.TcpTransport;
import io.riemann.riemann.client.IRiemannClient;
import io.riemann.riemann.client.ServerError;
import org.junit.Test;

import io.riemann.riemann.client.IPromise;
import io.riemann.riemann.client.OverloadedException;

import io.riemann.riemann.Proto.Attribute;
import io.riemann.riemann.Proto.Event;
import io.riemann.riemann.Proto.Msg;

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
  public void limiterLeakTest() throws IOException {
    final long delay = 0;     // Server time to process a message
    final Server server = new EchoServer(delay);
    IRiemannClient client = null;

    try {
      client = RiemannClient.tcp(server.start());
      TcpTransport transport = (TcpTransport) client.transport();
      int limit = 2;
      transport.writeLimiter = new Semaphore(limit);
      // Simulate the case where the server gets down after the client
      // connected and thus no channel is available
      transport.state = TcpTransport.State.CONNECTED;

      for (int i = 0; i < limit; i++) {
        try {
          client.event().service("foo").metric(1).send().deref();
          fail("Should throw IOException when there is no channel available");
        } catch (IOException e) {
          assertEquals("no channels available", e.getMessage());
        }
      }

      transport.state = TcpTransport.State.DISCONNECTED;
      client.connect();

      // Should not throw OverloadedException.
      client.event().service("foo").metric(1).send().deref();
    } finally {
      if (client != null) {
        client.close();
      }
      server.stop();
    }
  }

  @Test
  public void overloadTest() throws IOException {
    // Milliseconds
    final long delay = 10;     // Server time to process a message
    final long fast = 1;                         // Async latencies
    final double slow = ((double) delay) * 0.95; // Backpressure latencies

    final Server server = new EchoServer(delay);
    IRiemannClient client = null;

    try {
      client = RiemannClient.tcp(server.start());
      ((TcpTransport) client.transport()).setWriteBufferLimit(5);
      client.connect();

      final int n = 100000;
      final List<IPromise<Msg>> responses = new ArrayList<IPromise<Msg>>(n);
      long latency;
      long t0;

      // Queue up a bunch of writes
      for (int i = 0; i < n; i++) {
        // Measure the time it takes to call .send()
        t0 = System.nanoTime();
        responses.add(client.event().service("slow").metric(i).send());
        latency = System.nanoTime() - t0;
        if(latency > 100000000 ) {
          System.out.println("i : " + i + " ; " + latency);
        }
        assertTrue(latency <= 100000000);
      }

      // Deref all and spew out success/failure pairs
      // 0: success
      // 1: timeout
      // 2: overload
      // 3: other
      final ArrayList<int[]> results = new ArrayList<int[]>();
      int state = -1;
      int count = 0;
      long deadline = System.currentTimeMillis() + 1000;
      for (IPromise<Msg> response : responses) {
        try {
          if (null == response.deref(deadline - System.currentTimeMillis(),
                                     TimeUnit.MILLISECONDS)) {
            // Timeout
            if (state == -1) {
              state = 1;
            } else if (state != 1) {
              results.add(new int[]{state, count});
              state = 1;
            }
          } else {
            // OK
            if (state == -1) {
              state = 0;
            } else if (state != 0) {
              results.add(new int[]{state, count});
              state = 0;
              count = 0;
            }
          }
        } catch (OverloadedException e) {
          // Not OK
          if (state == -1) {
            state = 2;
          } else if (state != 2) {
            results.add(new int[]{state, count});
            state = 2;
            count = 0;
          }
        } catch (Exception e) {
          // Huh?
          if (state == -1) {
            state = 3;
          } else if (state != 3) {
            results.add(new int[]{state, count});
            state = 3;
            count = 0;
          }
        }
        count++;
      }

      // Print outcomes
      //for (int[] res : results) {
      //  if (res[0] == 0) {
      //    System.out.println("ok\t\t" + res[1]);
      //  } else if (res[0] == 1) {
      //    System.out.println("timeout\t" + res[1]);
      //  } else if (res[0] == 2) {
      //    System.out.println("overload\t" + res[1]);
      //  } else {
      //    System.out.println("other\t\t" + res[1]);
      //  }
      //}

      // OKs should come first
      assertTrue(0 == results.get(0)[0]);
      // Should be a lot of OKs
      assertTrue(10 < results.get(0)[1]);

      // Tally up totals
      int[] counts = new int[4];
      for (int[] res : results) {
        counts[res[0]] += res[1];
      }

      // Should see both overloads and timeouts
      assertTrue(0 < counts[1]);
      assertTrue(0 < counts[2]);

      // No others
      assertTrue(counts[3] == 0);
    } finally {
      if (client != null) {
        client.close();
      }
      server.stop();
    }
  }
}
