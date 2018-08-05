package com.codahale.metrics.riemann;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import io.riemann.riemann.client.EventDSL;
import io.riemann.riemann.client.IRiemannClient;
import io.riemann.riemann.client.Promise;

public class RiemannReporterTest {

    private final long timestamp = 1000198;

    private final List<String> tags = Arrays.asList("taga", "tagb");

    private final float ttl = 100;

    private final String localHost = "ME";

    private final Clock clock = mock(Clock.class);

    private final EventDSL eventDSL = mock(EventDSL.class, new Answer() {

        public Object answer(InvocationOnMock invocation) {
            if (invocation.getMethod().getName() == "send") {
                // We don't actually look for completion of the promise.
                return new Promise();
            } else {
                // Builder-style continuation
                return invocation.getMock();
            }
        }
    });

    private final IRiemannClient client = mock(IRiemannClient.class);

    private final Riemann riemann = spy(new Riemann(client));

    private final MetricRegistry registry = mock(MetricRegistry.class);

    private final ValueFilterMap valueFilterMap = new ValueFilterMap();

    private final RiemannReporter reporter = RiemannReporter
        .forRegistry(registry).withClock(clock).prefixedWith("prefix")
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS).filter(MetricFilter.ALL)
        .useSeparator("|").localHost(localHost).withTtl(ttl).tags(tags)
        .withValueFilterMap(valueFilterMap).build(riemann);

    @Before
    public void setUp()
        throws Exception {
        when(clock.getTime()).thenReturn(timestamp * 1000);
        when(client.event()).thenReturn(eventDSL);
        valueFilterMap.clear();
    }

    @Test
    public void uniqueEventsFromClosure()
        throws Exception {
        final Meter meter = mock(Meter.class);
        reporter.report(this.<Gauge> map(), this.<Counter> map(),
                        this.<Histogram> map(),
                        this.<Meter> map("meter", meter), this.<Timer> map());

        verify(client, times(5)).event();
    }

    @Test
    public void doesNotReportStringGaugeValues()
        throws Exception {
        reportGauge("value");
        verifyNoReporting();
    }

    @Test
    public void doesNotReportNullGaugeValues()
        throws Exception {
        reportGauge(null);
        verifyNoReporting();
    }

    private void verifyNoReporting()
        throws IOException {
        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        inOrder.verify(client, never()).event();
        verifyZeroInteractions(eventDSL);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    private void reportGauge(Object value) {
        reporter.report(map("gauge", gauge(value)), this.<Counter> map(),
                        this.<Histogram> map(), this.<Meter> map(),
                        this.<Timer> map());
    }

    @Test
    public void reportsByteGaugeValues()
        throws Exception {
        reporter.report(map("gauge", gauge((byte) 1)), this.<Counter> map(),
                        this.<Histogram> map(), this.<Meter> map(),
                        this.<Timer> map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|gauge", (byte) 1, null);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsShortGaugeValues()
        throws Exception {
        reporter.report(map("gauge", gauge((short) 1)), this.<Counter> map(),
                        this.<Histogram> map(), this.<Meter> map(),
                        this.<Timer> map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|gauge", (short) 1, null);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsIntegerGaugeValues()
        throws Exception {
        reporter.report(map("gauge", gauge(1)), this.<Counter> map(),
                        this.<Histogram> map(), this.<Meter> map(),
                        this.<Timer> map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|gauge", 1, null);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsLongGaugeValues()
        throws Exception {
        reporter.report(map("gauge", gauge(1L)), this.<Counter> map(),
                        this.<Histogram> map(), this.<Meter> map(),
                        this.<Timer> map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|gauge", 1L, null);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsFloatGaugeValues()
        throws Exception {
        reporter.report(map("gauge", gauge(1.1f)), this.<Counter> map(),
                        this.<Histogram> map(), this.<Meter> map(),
                        this.<Timer> map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|gauge", 1.1f, null);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsDoubleGaugeValues()
        throws Exception {
        reporter.report(map("gauge", gauge(1.1)), this.<Counter> map(),
                        this.<Histogram> map(), this.<Meter> map(),
                        this.<Timer> map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|gauge", 1.1, null);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();

        verifyNoMoreInteractions(riemann);
    }

    @Test
    public void reportsCounters()
        throws Exception {
        final Counter counter = mock(Counter.class);
        // Warn on over 50, critical over 150
        valueFilterMap
            .addFilter(counter, "count",
                 new ValueFilter.Builder("warn").withLower(50).build())
            .addFilter(counter, "count",
                 new ValueFilter.Builder("critical").withLower(150).build());
        // Value is 100, so should be warn
        when(counter.getCount()).thenReturn(100L);

        reporter.report(this.<Gauge> map(),
                        this.<Counter> map("counter", counter),
                        this.<Histogram> map(), this.<Meter> map(),
                        this.<Timer> map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|counter|count", 100L, "warn");
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsHistograms()
        throws Exception {
        final Histogram histogram = mock(Histogram.class);
        when(histogram.getCount()).thenReturn(1L);

        final Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.getMax()).thenReturn(2L);
        when(snapshot.getMean()).thenReturn(3.0);
        when(snapshot.getMin()).thenReturn(4L);
        when(snapshot.getStdDev()).thenReturn(5.0);
        when(snapshot.getMedian()).thenReturn(6.0);
        when(snapshot.get75thPercentile()).thenReturn(7.0);
        when(snapshot.get95thPercentile()).thenReturn(8.0);
        when(snapshot.get98thPercentile()).thenReturn(9.0);
        when(snapshot.get99thPercentile()).thenReturn(10.0);
        when(snapshot.get999thPercentile()).thenReturn(11.0);

        when(histogram.getSnapshot()).thenReturn(snapshot);

        reporter.report(this.<Gauge> map(), this.<Counter> map(),
                        this.<Histogram> map("histogram", histogram),
                        this.<Meter> map(), this.<Timer> map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|histogram|count", 1L, "ok");
        verifyInOrder(inOrder, "prefix|histogram|max", 2L, "ok");
        verifyInOrder(inOrder, "prefix|histogram|mean", 3.0, "ok");
        verifyInOrder(inOrder, "prefix|histogram|min", 4L, "ok");
        verifyInOrder(inOrder, "prefix|histogram|stddev", 5.0, "ok");
        verifyInOrder(inOrder, "prefix|histogram|p50", 6.0, "ok");
        verifyInOrder(inOrder, "prefix|histogram|p75", 7.0, "ok");
        verifyInOrder(inOrder, "prefix|histogram|p95", 8.0, "ok");
        verifyInOrder(inOrder, "prefix|histogram|p98", 9.0, "ok");
        verifyInOrder(inOrder, "prefix|histogram|p99", 10.0, "ok");
        verifyInOrder(inOrder, "prefix|histogram|p999", 11.0, "ok");

        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsMeters()
        throws Exception {
        final Meter meter = mock(Meter.class);
        when(meter.getCount()).thenReturn(1L);
        when(meter.getOneMinuteRate()).thenReturn(2.0);
        when(meter.getFiveMinuteRate()).thenReturn(3.0);
        when(meter.getFifteenMinuteRate()).thenReturn(4.0);
        when(meter.getMeanRate()).thenReturn(5.0);

        reporter.report(this.<Gauge> map(), this.<Counter> map(),
                        this.<Histogram> map(),
                        this.<Meter> map("meter", meter), this.<Timer> map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|meter|count", 1L, "ok");
        verifyInOrder(inOrder, "prefix|meter|m1_rate", 2.0, "ok");
        verifyInOrder(inOrder, "prefix|meter|m5_rate", 3.0, "ok");
        verifyInOrder(inOrder, "prefix|meter|m15_rate", 4.0, "ok");
        verifyInOrder(inOrder, "prefix|meter|mean_rate", 5.0, "ok");
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsTimers()
        throws Exception {
        final Timer timer = mock(Timer.class);
        valueFilterMap
            .addFilter(timer, ValueFilterMap.MAX,
                 new ValueFilter.Builder("critical").withLower(50).build())
            .addFilter(timer, ValueFilterMap.MEAN, new ValueFilter.Builder("warn")
                .withUpperExclusive(300000000).withLower(100000000).build());
        when(timer.getCount()).thenReturn(1L);
        when(timer.getMeanRate()).thenReturn(2.0);
        when(timer.getOneMinuteRate()).thenReturn(3.0);
        when(timer.getFiveMinuteRate()).thenReturn(4.0);
        when(timer.getFifteenMinuteRate()).thenReturn(5.0);

        final Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.getMax()).thenReturn(TimeUnit.MILLISECONDS.toNanos(100));
        when(snapshot.getMean())
            .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(200));
        when(snapshot.getMin()).thenReturn(TimeUnit.MILLISECONDS.toNanos(300));
        when(snapshot.getStdDev())
            .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(400));
        when(snapshot.getMedian())
            .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(500));
        when(snapshot.get75thPercentile())
            .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(600));
        when(snapshot.get95thPercentile())
            .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(700));
        when(snapshot.get98thPercentile())
            .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(800));
        when(snapshot.get99thPercentile())
            .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(900));
        when(snapshot.get999thPercentile())
            .thenReturn((double) TimeUnit.MILLISECONDS.toNanos(1000));

        when(timer.getSnapshot()).thenReturn(snapshot);

        reporter.report(this.<Gauge> map(), this.<Counter> map(),
                        this.<Histogram> map(), this.<Meter> map(),
                        map("timer", timer));

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|timer|max", 100.00, "critical");
        verifyInOrder(inOrder, "prefix|timer|mean", 200.00, "warn");
        verifyInOrder(inOrder, "prefix|timer|min", 300.00, "ok");
        verifyInOrder(inOrder, "prefix|timer|stddev", 400.00, "ok");
        verifyInOrder(inOrder, "prefix|timer|p50", 500.00, "ok");
        verifyInOrder(inOrder, "prefix|timer|p75", 600.00, "ok");
        verifyInOrder(inOrder, "prefix|timer|p95", 700.00, "ok");
        verifyInOrder(inOrder, "prefix|timer|p98", 800.00, "ok");
        verifyInOrder(inOrder, "prefix|timer|p99", 900.00, "ok");
        verifyInOrder(inOrder, "prefix|timer|p999", 1000.00, "ok");
        verifyInOrder(inOrder, "prefix|timer|count", 1L, "ok");
        verifyInOrder(inOrder, "prefix|timer|m1_rate", 3.0, "ok");
        verifyInOrder(inOrder, "prefix|timer|m5_rate", 4.0, "ok");
        verifyInOrder(inOrder, "prefix|timer|m15_rate", 5.0, "ok");
        verifyInOrder(inOrder, "prefix|timer|mean_rate", 2.0, "ok");
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    private <T> SortedMap<String, T> map() {
        return new TreeMap<String, T>();
    }

    private <T> SortedMap<String, T> map(String name, T metric) {
        final TreeMap<String, T> map = new TreeMap<String, T>();
        map.put(name, metric);
        return map;
    }

    private <T> Gauge gauge(T value) {
        final Gauge gauge = mock(Gauge.class);
        when(gauge.getValue()).thenReturn(value);
        return gauge;
    }

    private void verifyInOrder(InOrder inOrder, String service, Object metric,
                               String state)
        throws Exception {
        inOrder.verify(client).event();
        inOrder.verify(eventDSL).host(localHost);
        inOrder.verify(eventDSL).ttl(ttl);
        inOrder.verify(eventDSL).tags(tags);
        inOrder.verify(eventDSL).service(service);
        inOrder.verify(eventDSL).time(timestamp);

        if (metric instanceof Float) {
            inOrder.verify(eventDSL).metric((Float) metric);
        } else if (metric instanceof Double) {
            inOrder.verify(eventDSL).metric((Double) metric);
        } else if (metric instanceof Byte) {
            inOrder.verify(eventDSL).metric((Byte) metric);
        } else if (metric instanceof Short) {
            inOrder.verify(eventDSL).metric((Short) metric);
        } else if (metric instanceof Integer) {
            inOrder.verify(eventDSL).metric((Integer) metric);
        } else if (metric instanceof Long) {
            inOrder.verify(eventDSL).metric((Long) metric);
        } else {
            throw new RuntimeException();
        }

        if (state != null) {
            inOrder.verify(eventDSL).state(state);
        }

        inOrder.verify(eventDSL).send();
    }
}
