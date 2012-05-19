package com.yammer.metrics.reporting;

import com.aphyr.riemann.client.*;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.stats.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class RiemannReporter extends AbstractPollingReporter implements MetricProcessor<Long> {
    private static final Logger LOG = LoggerFactory.getLogger(RiemannReporter.class);
    protected final Clock clock;
    protected final MetricPredicate predicate;
    protected final RiemannClient riemann;
    protected final String prefix;
    protected final String separator;
    protected final VirtualMachineMetrics vm;
    public boolean printVMMetrics = true;

    public static class Config {
        public MetricsRegistry metricsRegistry = Metrics.defaultRegistry();
        public MetricPredicate predicate = MetricPredicate.ALL;
        public boolean printVMMetrics = false;
        public String host = "localhost";
        public int port = 5555;
        public long period = 60;
        public TimeUnit unit = TimeUnit.SECONDS;
        public String prefix = null;
        public String separator = " ";
        public VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();
        public Clock clock = Clock.defaultClock();
        public String name = "riemann-reporter";

        public Config() {}

        public Config metricsRegistry(MetricsRegistry r) { metricsRegistry = r; return this; }
        public Config predicate(MetricPredicate p) { predicate = p; return this; }
        public Config printVMMetrics(Boolean p) { printVMMetrics = p; return this; }
        public Config host(String h) { host = h; return this; }
        public Config port(int p) { port = p; return this; }
        public Config period(long p) { period = p; return this; }
        public Config unit(TimeUnit t) { unit = t; return this; }
        public Config prefix(String p) { prefix = p; return this; }
        public Config separator(String s) { separator = s; return this; }
        public Config clock(Clock c) { clock = c; return this; }
        public Config name (String n) { name = n; return this; }
    }

    public static Config config() {
        return new Config();
    }

    public static void enable(Config config) {
        try {
            final RiemannReporter reporter = new RiemannReporter(config);
            reporter.start(config.period, config.unit);
        } catch (Exception e) {
            LOG.error("Error creating/starting Riemann reporter: ", e);
        }
    }

    public RiemannReporter(Config c) throws IOException {
        super(c.metricsRegistry, c.name);
        this.riemann = new RiemannClient(new InetSocketAddress(c.host, c.port));
        riemann.connect();
        this.predicate = c.predicate;
        this.printVMMetrics = c.printVMMetrics;
        this.prefix = c.prefix;
        this.separator = c.separator;
        this.vm = c.vm;
        this.clock = c.clock;
    }

    @Override
    public void run() {
        try {
            final long epoch = clock.time() / 1000;
            if (this.printVMMetrics) {
                sendVMMetrics(epoch);
            }

            sendRegularMetrics(epoch);
        } catch (Exception e) {
            LOG.warn("Error writing to Riemann", e);
        }
    }

    protected void sendRegularMetrics(final Long epoch) {
        for (Entry<String,SortedMap<MetricName,Metric>> entry : getMetricsRegistry().groupedMetrics(predicate).entrySet()) {
            for (Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
                final Metric metric = subEntry.getValue();
                if (metric != null) {
                    try {
                        metric.processWith(this, subEntry.getKey(), epoch);
                    } catch (Exception ignored) {
                        LOG.error("Error sending regular metric:", ignored);
                    }
                }
            }
        }
    }

    private EventDSL newEvent() {
        return riemann.event();
    }

    // The service name for a given metric.
    public String service(MetricName name, String... rest) {
        final StringBuilder sb = new StringBuilder();
        if (null != prefix) {
            sb.append(prefix).append(separator);
        }
        sb.append(name.getGroup())
                .append(separator)
                .append(name.getType())
                .append(separator);
        if (name.hasScope()) {
            sb.append(name.getScope())
              .append(separator);
        }
        sb.append(name.getName());
        for (String part : rest) {
            sb.append(separator);
            sb.append(part);
        }
        return sb.toString();
    }

    public String service(String... parts) {
        final StringBuilder sb = new StringBuilder();
        if (null != prefix) {
            sb.append(prefix).append(separator);
        }

        for (int i = 0; i < parts.length; i++) {
            sb.append(parts[i]);
            sb.append(separator);
        }

        return sb.substring(0, sb.length() - separator.length());
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Long epoch) {
        Object v = gauge.value();
        EventDSL e = newEvent().service(service(name)).time(epoch);
        if (v instanceof Integer) {
            e.metric((Integer) v).send();
        } else if (v instanceof Long) {
            e.metric((Long) v).send();
        } else if (v instanceof Double) {
            e.metric((Double) v).send();
        } else if (v instanceof Float) {
            e.metric((Float) v).send();
        } else if (v instanceof Number) {
            e.metric(((Number) v).floatValue()).send();
        }
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Long epoch) {
        newEvent()
                .service(service(name))
                .metric(counter.count())
                .time(epoch)
                .send();
    }

    @Override
    public void processMeter(MetricName name, Metered meter, Long epoch) {
        newEvent()
                .service(service(name))
                .metric(meter.oneMinuteRate())
                .time(epoch)
                .send();
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Long epoch) throws IOException {
        final String service = service(name);
        sendSummary(name, histogram, epoch);
        sendSample(name, histogram, epoch);
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Long epoch) {
        processMeter(name, timer, epoch);
        sendSummary(name, timer, epoch);
        sendSample(name, timer, epoch);
    }

    protected void sendVMMetrics(long epoch) {
        newEvent().time(epoch).service(service("jvm", "memory", "heap-usage")).metric(vm.heapUsage()).send();
        newEvent().time(epoch).service(service("jvm", "memory", "non-heap-usage")).metric(vm.nonHeapUsage()).send();
        for (Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
            newEvent().time(epoch).service(service("jvm", "memory", "pool-usage", pool.getKey())).metric(pool.getValue()).send();
        }
        newEvent().time(epoch).service(service("jvm", "thread", "daemon-count")).metric(vm.daemonThreadCount()).send();
        newEvent().time(epoch).service(service("jvm", "thread", "count")).metric(vm.threadCount()).send();
        newEvent().time(epoch).service(service("jvm", "uptime")).metric(vm.uptime()).send();
        newEvent().time(epoch).service(service("jvm", "fd-usage")).metric(vm.fileDescriptorUsage()).send();

        for(Entry<Thread.State, Double> entry : vm.threadStatePercentages().entrySet()) {
            newEvent().time(epoch).service(service("jvm", "thread", "state", entry.getKey().toString().toLowerCase())).metric(entry.getValue()).send();
        }

        for(Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
            newEvent().time(epoch).service(service("jvm", "gc", entry.getKey(), "time")).metric(entry.getValue().getTime(TimeUnit.MILLISECONDS)).send();
            newEvent().time(epoch).service(service("jvm", "gc", entry.getKey(), "runs")).metric(entry.getValue().getRuns()).send();
        }
    }

    public void sendSummary(MetricName name, Summarizable metric, Long epoch) {
        newEvent().time(epoch).service(service(name, "min")).metric(metric.min()).send();
        newEvent().time(epoch).service(service(name, "max")).metric(metric.max()).send();
        newEvent().time(epoch).service(service(name, "mean")).metric(metric.mean()).send();
        newEvent().time(epoch).service(service(name, "stddev")).metric(metric.stdDev()).send();
    }

    public void sendSample(MetricName name, Sampling metric, Long epoch) {
        final Snapshot s = metric.getSnapshot();
        newEvent().time(epoch).service(service(name, ".5")).metric(s.getMedian()).send();
        newEvent().time(epoch).service(service(name, ".75")).metric(s.get75thPercentile()).send();
        newEvent().time(epoch).service(service(name, ".95")).metric(s.get95thPercentile()).send();
        newEvent().time(epoch).service(service(name, ".98")).metric(s.get98thPercentile()).send();
        newEvent().time(epoch).service(service(name, ".99")).metric(s.get99thPercentile()).send();
        newEvent().time(epoch).service(service(name, ".999")).metric(s.get999thPercentile()).send();
    }
}
