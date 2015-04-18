package riemann.java.client.tests;

import com.yammer.metrics.reporting.RiemannReporter;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class RiemannReporterConfigTest {

    private RiemannReporter.ConfigBuilder builder;

    @Before
    public void setUp() {
        builder = RiemannReporter.Config.newBuilder();
    }

    @Test
    public void testDefaultConfigBuilding() {
        RiemannReporter.Config c = builder.build();
        assertEquals(c.port, 5555);
        assertEquals(c.unit, TimeUnit.SECONDS);
        assertEquals(c.separator, " ");
        assertEquals(c.host, "localhost");
        assertTrue(c.tags.isEmpty());
    }

    @Test
    public void testConfigBuilding() {
        RiemannReporter.Config c = builder.host("127.0.0.1")
                                          .unit(TimeUnit.MILLISECONDS)
                                          .port(9999)
                                          .separator("#")
                                          .host("riemann")
                                          .build();

        assertEquals(c.port, 9999);
        assertEquals(c.unit, TimeUnit.MILLISECONDS);
        assertEquals(c.separator, "#");
        assertEquals(c.host, "riemann");

    }

    @Test
    public void testTagsConfigBuilding() {
        RiemannReporter.Config c = builder.tags(new HashSet<String>(){{ add("abc"); add("123"); }})
                                          .build();
        assertTrue(c.tags.contains("abc"));
        assertTrue(c.tags.contains("123"));
    }
}
