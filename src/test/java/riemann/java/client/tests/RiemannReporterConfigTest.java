package riemann.java.client.tests;

import com.yammer.metrics.reporting.RiemannReporter;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;

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

}
