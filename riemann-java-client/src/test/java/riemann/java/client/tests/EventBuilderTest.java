package riemann.java.client.tests;

import com.google.protobuf.ProtocolStringList;
import io.riemann.riemann.Proto;
import io.riemann.riemann.client.EventBuilder;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class EventBuilderTest {

    @Test
    public void should_create_event() {
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("network_zone", "dmz");
        List<String> tags = new ArrayList<String>();
        tags.add("fast");
        Proto.Event event = new EventBuilder()
                .host("riemann.localhost")
                .metric(42.2)
                .service("cpu_percent_usage")
                .state("fine")
                .tags("high", "backend")
                .tag("storage")
                .tags(tags)
                .description("Processor usage")
                .attribute("health", "good")
                .attribute("environment", "production")
                .attribute("datacenter", "eu_west")
                .attributes(attributes)
                .ttl(60)
                .build();

        assertEquals("cpu_percent_usage", event.getService());
        assertEquals("riemann.localhost", event.getHost());
        assertEquals("fine", event.getState());
        assertEquals(42.2, event.getMetricD(), 0D);
        ProtocolStringList eventTags = event.getTagsList();
        assertEquals(4, eventTags.size());
        assertEquals("high", eventTags.get(0));
        assertEquals("backend", eventTags.get(1));
        assertEquals("storage", eventTags.get(2));
        assertEquals("fast", eventTags.get(3));
        assertEquals("Processor usage", event.getDescription());
        List<Proto.Attribute> eventAttributes = getAttributesSortedByKey(event);
        assertEquals(4, eventAttributes.size());
        // datacenter -> eu_west
        assertEquals("datacenter", eventAttributes.get(0).getKey());
        assertEquals("eu_west", eventAttributes.get(0).getValue());
        // environment -> production
        assertEquals("environment", eventAttributes.get(1).getKey());
        assertEquals("production", eventAttributes.get(1).getValue());
        // health -> good
        assertEquals("health", eventAttributes.get(2).getKey());
        assertEquals("good", eventAttributes.get(2).getValue());
        // health -> good
        assertEquals("network_zone", eventAttributes.get(3).getKey());
        assertEquals("dmz", eventAttributes.get(3).getValue());
        assertEquals(60F, event.getTtl(), 0F);
    }

    @Test
    public void should_create_event_with_metric_float() {
        Proto.Event event = new EventBuilder()
                .metric(123.4F)
                .build();

        assertEquals(123.4F, event.getMetricF(), 0F);
    }

    @Test
    public void should_create_event_with_metric_int() {
        Proto.Event event = new EventBuilder()
                .metric(123)
                .build();

        assertEquals(123, event.getMetricSint64());
    }

    @Test
    public void should_create_event_with_metric_short() {
        Proto.Event event = new EventBuilder()
                .metric((short) 1)
                .build();

        assertEquals(1, event.getMetricSint64());
    }

    @Test
    public void should_create_event_with_metric_long() {
        Proto.Event event = new EventBuilder()
                .metric(1234567891011L)
                .build();

        assertEquals(1234567891011L, event.getMetricSint64());
    }

    @Test
    public void should_create_event_with_metric_byte() {
        Proto.Event event = new EventBuilder()
                .metric((byte) 1)
                .build();

        assertEquals(1, event.getMetricSint64());
    }

    @Test
    public void should_clear_metric() {
        EventBuilder eventBuilder = new EventBuilder();
        Proto.Event event = eventBuilder
                .host("riemann.localhost")
                .metric(123)
                .metric()
                .build();

        assertEquals(false, event.hasMetricD());
        assertEquals(false, event.hasMetricF());
        assertEquals(false, event.hasMetricSint64());
        assertEquals(0D, event.getMetricD(), 0D);
        assertEquals(0F, event.getMetricF(), 0F);
        assertEquals(0L, event.getMetricSint64(), 0L);
    }

    @Test
    public void should_clear_time() {
        EventBuilder eventBuilder = new EventBuilder();
        Proto.Event event = eventBuilder
                .host("riemann.localhost")
                .time(System.currentTimeMillis())
                .time()
                .build();

        assertEquals(false, event.hasTime());
        assertEquals(0L, event.getTime());
        assertEquals(0L, event.getTimeMicros());
    }

    @Test
    public void should_clear_ttl() {
        EventBuilder eventBuilder = new EventBuilder();
        Proto.Event event = eventBuilder
                .host("riemann.localhost")
                .ttl(20)
                .ttl()
                .build();

        assertEquals(false, event.hasTtl());
        assertEquals(0F, event.getTtl(), 0F);
    }

    private List<Proto.Attribute> getAttributesSortedByKey(Proto.Event event) {
        List<Proto.Attribute> attributes = new ArrayList<Proto.Attribute>(event.getAttributesList());
        Collections.sort(attributes, new Comparator<Proto.Attribute>() {
            @Override
            public int compare(Proto.Attribute o1, Proto.Attribute o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        return attributes;
    }
}
