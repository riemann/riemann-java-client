package riemann.java.client.tests;

import io.riemann.riemann.Proto;
import io.riemann.riemann.client.EventBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EventBuilderTest {

    @Test
    public void should_create_event() {
        EventBuilder eventBuilder = new EventBuilder();
        Proto.Event event = eventBuilder
                .host("riemann.localhost")
                .metric(42.2)
                .service("cpu_percent_usage")
                .state("fine")
                .tags("high", "backend")
                .description("Processor usage")
                .attribute("health", "good")
                .attribute("environment", "production")
                .attribute("datacenter", "eu_west")
                .ttl(60)
                .build();
        assertEquals("cpu_percent_usage", event.getService());
        assertEquals("riemann.localhost", event.getHost());
        assertEquals("fine", event.getState());
    }
}
