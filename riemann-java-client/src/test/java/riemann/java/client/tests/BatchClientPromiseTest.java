package riemann.java.client.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import io.riemann.riemann.client.IPromise;
import io.riemann.riemann.client.IRiemannClient;
import io.riemann.riemann.client.RiemannBatchClient;
import io.riemann.riemann.client.RiemannClient;
import io.riemann.riemann.client.ServerError;
import io.riemann.riemann.client.UnsupportedJVMException;

import io.riemann.riemann.Proto.Event;
import io.riemann.riemann.Proto.Msg;

public class BatchClientPromiseTest {
  @Test
  public void sendEventsTest() throws Exception, IOException, InterruptedException, ServerError, UnsupportedJVMException {
    final ArrayList<Event> events = new ArrayList<Event>();
    final ArrayList<IPromise<Msg>> promises = new ArrayList<IPromise<Msg>>();

    final Server server = new OkServer();
    IRiemannClient client = null;

    final int BATCH_SIZE = 10;
    final int NUM_EVENTS = 15;
    try {
      client = new RiemannBatchClient(RiemannClient.tcp(server.start()),
                                      BATCH_SIZE);
      client.connect();
      {
        final Event e = Util.createEvent();
        events.add(e);
        IPromise<Msg> promise = client.sendEvent(e);
        // First event should be sitting in the buffer, not sent yet.
        assertEquals(null, promise.deref(10, (Object) null));
        promises.add(promise);
      }
      for (int i = 1; i < NUM_EVENTS; i++) {
        final Event e = Util.createEvent();
        events.add(e);
        promises.add(client.sendEvent(e));
      }
      client.flush();
      for (int i = 0; i < events.size(); i++) {
        Msg rsp = promises.get(i).deref(100, TimeUnit.MILLISECONDS,
                                        Msg.newBuilder().setOk(false).build());
        assertTrue(!rsp.hasOk() || rsp.getOk());
      }
      for (int i = 0; i < events.size(); ) {
        final int expecting = Math.min(events.size() - i, BATCH_SIZE);
        Msg recv = server.received.poll();
        assertEquals(expecting, recv.getEventsCount());
        for (int j = 0; j < expecting; i++, j++) {
          assertEquals(events.get(i), recv.getEvents(j));
        }
      }
    } finally {
      if (client != null) {
        client.close();
      }
      server.stop();
    }
  }
}
