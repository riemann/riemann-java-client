package riemann.java.client.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
	public void echoTest() throws IOException, InterruptedException, ServerError {
    final Server server = new EchoServer();
    RiemannClient client = null;
    try {
      client = RiemannClient.tcp(server.start());
      client.connect();
      final Event e = Util.createEvent();
      assertEquals(true, client.sendEventsWithAck(e));
    } finally {
      if (client != null) {
        client.disconnect();
      }
      server.stop();
    }
  }
}
