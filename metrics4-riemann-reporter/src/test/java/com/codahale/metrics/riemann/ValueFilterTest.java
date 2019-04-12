package com.codahale.metrics.riemann;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ValueFilterTest {

    @Test
    public void testApplies() {
        final ValueFilter warnFilter = new ValueFilter.Builder("warn")
            .withLower(900).withUpper(1000).build();
        final ValueFilter criticalFilter = new ValueFilter.Builder("critical")
            .withLowerExclusive(1000).build();
        final ValueFilter halfOpenFilter = new ValueFilter.Builder("warn")
            .withUpperExclusive(300).withLower(100).build();
        assertTrue(warnFilter.applies(920));
        assertFalse(warnFilter.applies(1001));
        assertTrue(criticalFilter.applies(1001));
        assertFalse(criticalFilter.applies(950));
        assertTrue(halfOpenFilter.applies(200));
    }

}
