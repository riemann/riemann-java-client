package riemann.java.client.tests;

import io.riemann.riemann.client.HashedWheelTimerFactory;
import io.riemann.riemann.client.Promise;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class HashedWheelTimerFactoryTest {

    @Test
    public void shouldCreateDaemonThreads() throws IOException {
        Thread thread = HashedWheelTimerFactory.daemonThreadFactory.newThread(null);
        assertTrue("Thread should be a daemon thread", thread.isDaemon());
    }

}
