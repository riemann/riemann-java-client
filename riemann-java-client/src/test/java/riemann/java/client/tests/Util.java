package riemann.java.client.tests;

import io.riemann.riemann.Proto.Attribute;
import io.riemann.riemann.Proto.Event;
import io.riemann.riemann.Proto.Msg;
import java.util.*;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class Util {
	public static Event createEvent() {
		final Random random = new Random();
		return Event.newBuilder()
				.setDescription("desc")
				.setHost("127.0.0.1")
				.setService("service")
				.setTime(random.nextInt(1000))
				.setTtl(random.nextInt(1000))
				.setMetricF(random.nextInt(1000))
				.addAllTags(Arrays.asList("tag1", "tag2"))
				.addAttributes(Attribute.newBuilder().setKey("key1").setValue("value1"))
				.build();
	}

  // Returns a single event from a message, asserting that it has only one
  // event.
  public static Event soleEvent(final Msg m) {
    assertEquals(1, m.getEventsCount());
    return m.getEvents(0);
  }

	public static void compareEvents(final Event e1, final Event e2) {
		assertEquals(e1.getHost(), e2.getHost());
		assertEquals(e1.getService(), e2.getService());
		assertEquals(e1.getState(), e2.getState());
		assertEquals(e1.getDescription(), e2.getDescription());
		assertEquals(e1.getMetricF(), e2.getMetricF(), 0);
		assertEquals(e1.getTime(), e2.getTime(), 0);
		assertEquals(e1.getTtl(), e2.getTtl(), 0);
		assertEquals(e1.getTagsList(), e2.getTagsList());
		assertEquals(e1.getAttributesList(), e2.getAttributesList());
	}
}
