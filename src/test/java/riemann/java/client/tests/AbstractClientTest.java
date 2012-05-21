package riemann.java.client.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.aphyr.riemann.client.AbstractRiemannClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.Proto.Msg;

public abstract class AbstractClientTest {

	private int port;
	private Thread server;
	private final AtomicReference<Msg> received = new AtomicReference<Msg>();
	private CountDownLatch serverStart;
	private CountDownLatch serverReceived;

	@Before
	public void start() {
		this.received.set(null);
		this.serverStart = new CountDownLatch(1);
		this.serverReceived = new CountDownLatch(1);
		this.port = 9800 + new Random().nextInt(100);
		this.server = createServer(this.port, this.received);
		this.server.start();
	}

	@After
	public void stop() {
		this.server.interrupt();
		this.server = null;
		this.port = -1;
		this.received.set(null);
		this.serverStart = null;
	}

	protected void serverStarted() {
		this.serverStart.countDown();
	}

	protected void serverReceived() {
		this.serverReceived.countDown();
	}

	abstract Thread createServer(int port, AtomicReference<Msg> received);

	abstract AbstractRiemannClient createClient(int port) throws IOException;

	@Test
	public void sendEvent() throws IOException, InterruptedException {
		final Event sent = createEvent();
		final AbstractRiemannClient client = createClient(this.port);
		if (this.serverStart.await(500, TimeUnit.MILLISECONDS)) {
			client.connect();
			client.sendEvents(sent);
			if (this.serverReceived.await(500, TimeUnit.MILLISECONDS)) {
				client.disconnect();
				compareEvents(sent, this.received.get());
			} else {
				fail("Timed out witing for server to receive message");
			}
		} else {
			fail("Timed out waiting for server to start");
		}
	}

	private static Event createEvent() {
		final Random random = new Random();
		return Event.newBuilder()
				.setDescription("desc")
				.setHost("127.0.0.1")
				.setService("service")
				.setTime(random.nextInt(1000))
				.setTtl(random.nextInt(1000))
				.setMetricF(random.nextInt(1000))
				.addAllTags(Arrays.asList("tag1", "tag2"))
				.build();
	}

	private static void compareEvents(final Event sent, final Msg message) {
		assertEquals(1, message.getEventsCount());
		final Event parsed = message.getEvents(0);
		assertEquals(sent.getHost(), parsed.getHost());
		assertEquals(sent.getService(), parsed.getService());
		assertEquals(sent.getState(), parsed.getState());
		assertEquals(sent.getDescription(), parsed.getDescription());
		assertEquals(sent.getMetricF(), parsed.getMetricF(), 0);
		assertEquals(sent.getTime(), parsed.getTime(), 0);
		assertEquals(sent.getTtl(), parsed.getTtl(), 0);
		assertEquals(sent.getTagsList(), parsed.getTagsList());
	}
}
