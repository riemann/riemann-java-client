package riemann.java.client.tests;

import com.aphyr.riemann.Proto.Attribute;
import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.Proto.Msg;
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

	public static void compareEvents(final Event sent, final Msg message) {
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
		assertEquals(sent.getAttributesList(), parsed.getAttributesList());
	}
}
