package com.codahale.metrics.riemann;

import com.aphyr.riemann.client.IRiemannClient;
import com.aphyr.riemann.client.EventDSL;
import com.aphyr.riemann.client.Promise;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;

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
    private final RiemannReporter reporter = RiemannReporter.forRegistry(registry)
                                                              .withClock(clock)
                                                              .prefixedWith("prefix")
                                                              .convertRatesTo(TimeUnit.SECONDS)
                                                              .convertDurationsTo(TimeUnit.MILLISECONDS)
                                                              .filter(MetricFilter.ALL)
                                                              .useSeparator("|")
                                                              .localHost(localHost)
                                                              .withTtl(ttl)
                                                              .tags(tags)
                                                              .build(riemann);

    @Before
    public void setUp() throws Exception {
        when(clock.getTime()).thenReturn(timestamp * 1000);
        when(client.event()).thenReturn(eventDSL);
    }

    @Test
    public void uniqueEventsFromClosure() throws Exception {
        final Meter meter = mock(Meter.class);
        reporter.report(this.<Gauge>map(),
                        this.<Counter>map(),
                        this.<Histogram>map(),
                        this.<Meter>map("meter", meter),
                        this.<Timer>map());

        verify(client, times(5)).event();
    }

    @Test
    public void doesNotReportStringGaugeValues() throws Exception {
        reporter.report(map("gauge", gauge("value")),
                        this.<Counter>map(),
                        this.<Histogram>map(),
                        this.<Meter>map(),
                        this.<Timer>map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        inOrder.verify(client, never()).event();
        verifyZeroInteractions(eventDSL);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsByteGaugeValues() throws Exception {
        reporter.report(map("gauge", gauge((byte) 1)),
                        this.<Counter>map(),
                        this.<Histogram>map(),
                        this.<Meter>map(),
                        this.<Timer>map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|gauge", (byte) 1);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsShortGaugeValues() throws Exception {
        reporter.report(map("gauge", gauge((short) 1)),
                        this.<Counter>map(),
                        this.<Histogram>map(),
                        this.<Meter>map(),
                        this.<Timer>map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|gauge", (short) 1);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsIntegerGaugeValues() throws Exception {
        reporter.report(map("gauge", gauge(1)),
                        this.<Counter>map(),
                        this.<Histogram>map(),
                        this.<Meter>map(),
                        this.<Timer>map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|gauge", 1);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsLongGaugeValues() throws Exception {
        reporter.report(map("gauge", gauge(1L)),
                        this.<Counter>map(),
                        this.<Histogram>map(),
                        this.<Meter>map(),
                        this.<Timer>map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|gauge", 1L);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsFloatGaugeValues() throws Exception {
        reporter.report(map("gauge", gauge(1.1f)),
                        this.<Counter>map(),
                        this.<Histogram>map(),
                        this.<Meter>map(),
                        this.<Timer>map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|gauge", 1.1f);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsDoubleGaugeValues() throws Exception {
        reporter.report(map("gauge", gauge(1.1)),
                        this.<Counter>map(),
                        this.<Histogram>map(),
                        this.<Meter>map(),
                        this.<Timer>map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|gauge", 1.1);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();

        verifyNoMoreInteractions(riemann);
    }

    @Test
    public void reportsCounters() throws Exception {
        final Counter counter = mock(Counter.class);
        when(counter.getCount()).thenReturn(100L);

        reporter.report(this.<Gauge>map(),
                        this.<Counter>map("counter", counter),
                        this.<Histogram>map(),
                        this.<Meter>map(),
                        this.<Timer>map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|counter|count", 100L);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsHistograms() throws Exception {
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

        reporter.report(this.<Gauge>map(),
                        this.<Counter>map(),
                        this.<Histogram>map("histogram", histogram),
                        this.<Meter>map(),
                        this.<Timer>map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|histogram|count", 1L);
        verifyInOrder(inOrder, "prefix|histogram|max", 2L);
        verifyInOrder(inOrder, "prefix|histogram|mean", 3.0);
        verifyInOrder(inOrder, "prefix|histogram|min", 4L);
        verifyInOrder(inOrder, "prefix|histogram|stddev", 5.0);
        verifyInOrder(inOrder, "prefix|histogram|p50", 6.0);
        verifyInOrder(inOrder, "prefix|histogram|p75", 7.0);
        verifyInOrder(inOrder, "prefix|histogram|p95", 8.0);
        verifyInOrder(inOrder, "prefix|histogram|p98", 9.0);
        verifyInOrder(inOrder, "prefix|histogram|p99", 10.0);
        verifyInOrder(inOrder, "prefix|histogram|p999", 11.0);

        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsMeters() throws Exception {
        final Meter meter = mock(Meter.class);
        when(meter.getCount()).thenReturn(1L);
        when(meter.getOneMinuteRate()).thenReturn(2.0);
        when(meter.getFiveMinuteRate()).thenReturn(3.0);
        when(meter.getFifteenMinuteRate()).thenReturn(4.0);
        when(meter.getMeanRate()).thenReturn(5.0);

        reporter.report(this.<Gauge>map(),
                        this.<Counter>map(),
                        this.<Histogram>map(),
                        this.<Meter>map("meter", meter),
                        this.<Timer>map());

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|meter|count", 1L);
        verifyInOrder(inOrder, "prefix|meter|m1_rate", 2.0);
        verifyInOrder(inOrder, "prefix|meter|m5_rate", 3.0);
        verifyInOrder(inOrder, "prefix|meter|m15_rate", 4.0);
        verifyInOrder(inOrder, "prefix|meter|mean_rate", 5.0);
        inOrder.verify(client).flush();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void reportsTimers() throws Exception {
        final Timer timer = mock(Timer.class);
        when(timer.getCount()).thenReturn(1L);
        when(timer.getMeanRate()).thenReturn(2.0);
        when(timer.getOneMinuteRate()).thenReturn(3.0);
        when(timer.getFiveMinuteRate()).thenReturn(4.0);
        when(timer.getFifteenMinuteRate()).thenReturn(5.0);

        final Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.getMax()).thenReturn(TimeUnit.MILLISECONDS.toNanos(100));
        when(snapshot.getMean()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(200));
        when(snapshot.getMin()).thenReturn(TimeUnit.MILLISECONDS.toNanos(300));
        when(snapshot.getStdDev()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(400));
        when(snapshot.getMedian()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(500));
        when(snapshot.get75thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(600));
        when(snapshot.get95thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(700));
        when(snapshot.get98thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(800));
        when(snapshot.get99thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS.toNanos(900));
        when(snapshot.get999thPercentile()).thenReturn((double) TimeUnit.MILLISECONDS
                                                                        .toNanos(1000));

        when(timer.getSnapshot()).thenReturn(snapshot);

        reporter.report(this.<Gauge>map(),
                        this.<Counter>map(),
                        this.<Histogram>map(),
                        this.<Meter>map(),
                        map("timer", timer));

        final InOrder inOrder = inOrder(riemann, eventDSL, client);
        inOrder.verify(riemann).connect();
        verifyInOrder(inOrder, "prefix|timer|max", 100.00);
        verifyInOrder(inOrder, "prefix|timer|mean", 200.00);
        verifyInOrder(inOrder, "prefix|timer|min", 300.00);
        verifyInOrder(inOrder, "prefix|timer|stddev", 400.00);
        verifyInOrder(inOrder, "prefix|timer|p50", 500.00);
        verifyInOrder(inOrder, "prefix|timer|p75", 600.00);
        verifyInOrder(inOrder, "prefix|timer|p95", 700.00);
        verifyInOrder(inOrder, "prefix|timer|p98", 800.00);
        verifyInOrder(inOrder, "prefix|timer|p99", 900.00);
        verifyInOrder(inOrder, "prefix|timer|p999", 1000.00);
        verifyInOrder(inOrder, "prefix|timer|count", 1L);
        verifyInOrder(inOrder, "prefix|timer|m1_rate", 3.0);
        verifyInOrder(inOrder, "prefix|timer|m5_rate", 4.0);
        verifyInOrder(inOrder, "prefix|timer|m15_rate", 5.0);
        verifyInOrder(inOrder, "prefix|timer|mean_rate", 2.0);
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

    private void verifyInOrder(InOrder inOrder, String service, Object metric) throws Exception {
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

        inOrder.verify(eventDSL).send();
    }
}
