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
    protected final RiemannClient riemann;
    protected final Config c;

    public static class Config {

        public final MetricsRegistry metricsRegistry;
        public final MetricPredicate predicate;
        public final boolean printVMMetrics;
        public final String host;
        public final int port;
        public final long period;
        public final TimeUnit unit;
        public final String prefix;
        public final String separator;
        public final VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();
        public final Clock clock;
        public final String name;
        public final String localHost;

        private Config(MetricsRegistry metricsRegistry, MetricPredicate predicate, boolean printVMMetrics,
                       String host, int port, long period, TimeUnit unit, String prefix, String separator,
                       Clock clock, String name, String localHost) {
            this.metricsRegistry = metricsRegistry;
            this.predicate = predicate;
            this.printVMMetrics = printVMMetrics;
            this.host = host;
            this.port = port;
            this.period = period;
            this.unit = unit;
            this.prefix = prefix;
            this.separator = separator;
            this.clock = clock;
            this.name = name;
            this.localHost = localHost;

        }

        public static ConfigBuilder newBuilder() {
            return new ConfigBuilder();
        }

        @Override
        public String toString() {
            return "Config{" +
                    "print_metrics:" + printVMMetrics + ", host:" + host + ", port:" + port + ", period:" + period +
                    ", unit:" + unit + ", prefix:" + prefix + ", separator:" + separator + ", clock:" + clock +
                    ", name:" + name + ", localhost:" + localHost + ", metrics_registry:" + metricsRegistry +
                    ", predicate:" + predicate + "}";
        }
    }

    public static final class ConfigBuilder {
        // Default values for Config
        private MetricsRegistry metricsRegistry = Metrics.defaultRegistry();
        private MetricPredicate predicate = MetricPredicate.ALL;
        private boolean printVMMetrics = true;
        private String host = "localhost";
        private int port = 5555;
        private long period = 60;
        private TimeUnit unit = TimeUnit.SECONDS;
        private String prefix = null;
        private String separator = " ";
        private final VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();
        private Clock clock = Clock.defaultClock();
        private String name = "riemann-reporter";
        private String localHost = null;

        private ConfigBuilder() { }

        public Config build(){
            return new Config(metricsRegistry, predicate, printVMMetrics, host, port,
                              period, unit, prefix, separator, clock, name, localHost);
        }

        public ConfigBuilder metricsRegistry(MetricsRegistry r) { metricsRegistry = r; return this; }
        public ConfigBuilder predicate(MetricPredicate p) { predicate = p; return this; }
        public ConfigBuilder printVMMetrics(Boolean p) { printVMMetrics = p; return this; }
        public ConfigBuilder host(String h) { host = h; return this; }
        public ConfigBuilder port(int p) { port = p; return this; }
        public ConfigBuilder period(long p) { period = p; return this; }
        public ConfigBuilder unit(TimeUnit t) { unit = t; return this; }
        public ConfigBuilder prefix(String p) { prefix = p; return this; }
        public ConfigBuilder separator(String s) { separator = s; return this; }
        public ConfigBuilder clock(Clock c) { clock = c; return this;        }
        public ConfigBuilder name(String n) { name = n; return this; }
        public ConfigBuilder localHost(String l) { localHost = l; return this; }
    }

    public static void enable(final Config config) {
        try {
            if (config == null)
                throw new IllegalArgumentException("Config cannot be null");

            final RiemannReporter reporter = new RiemannReporter(config);
            reporter.start(config.period, config.unit);
        } catch (Exception e) {
            LOG.error("Error creating/starting Riemann reporter: ", e);
        }
    }

    public RiemannReporter(final Config c) throws IOException {
        super(c.metricsRegistry, c.name);
        this.riemann = new RiemannClient(new InetSocketAddress(c.host, c.port));
        riemann.connect();
        this.c = c;
    }

    @Override
    public void run() {
        try {
            final long epoch = c.clock.time() / 1000;
            if (c.printVMMetrics) {
                sendVMMetrics(epoch);
            }

            sendRegularMetrics(epoch);
        } catch (Exception e) {
            LOG.warn("Error writing to Riemann", e);
        }
    }

    protected void sendRegularMetrics(final Long epoch) {
        for (Entry<String,SortedMap<MetricName,Metric>> entry : getMetricsRegistry().groupedMetrics(c.predicate).entrySet()) {
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
        EventDSL event = riemann.event();
        if (c.localHost != null) {
            event.host(c.localHost);
        }
        return event;
    }

    // The service name for a given metric.
    public String service(MetricName name, String... rest) {
        final StringBuilder sb = new StringBuilder();
        if (null != c.prefix) {
            sb.append(c.prefix).append(c.separator);
        }
        sb.append(name.getGroup())
                .append(c.separator)
                .append(name.getType())
                .append(c.separator);
        if (name.hasScope()) {
            sb.append(name.getScope())
              .append(c.separator);
        }
        sb.append(name.getName());
        for (String part : rest) {
            sb.append(c.separator);
            sb.append(part);
        }
        return sb.toString();
    }

    public String service(String... parts) {
        final StringBuilder sb = new StringBuilder();
        if (null != c.prefix) {
            sb.append(c.prefix).append(c.separator);
        }

        for (String p : parts) {
            sb.append(p).append(c.separator);
        }

        return sb.substring(0, sb.length() - c.separator.length());
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
        newEvent().time(epoch).service(service("jvm", "memory", "heap-usage")).metric(c.vm.heapUsage()).send();
        newEvent().time(epoch).service(service("jvm", "memory", "non-heap-usage")).metric(c.vm.nonHeapUsage()).send();
        for (Entry<String, Double> pool : c.vm.memoryPoolUsage().entrySet()) {
            newEvent().time(epoch).service(service("jvm", "memory", "pool-usage", pool.getKey())).metric(pool.getValue()).send();
        }
        newEvent().time(epoch).service(service("jvm", "thread", "daemon-count")).metric(c.vm.daemonThreadCount()).send();
        newEvent().time(epoch).service(service("jvm", "thread", "count")).metric(c.vm.threadCount()).send();
        newEvent().time(epoch).service(service("jvm", "uptime")).metric(c.vm.uptime()).send();
        newEvent().time(epoch).service(service("jvm", "fd-usage")).metric(c.vm.fileDescriptorUsage()).send();

        for(Entry<Thread.State, Double> entry : c.vm.threadStatePercentages().entrySet()) {
            newEvent().time(epoch).service(service("jvm", "thread", "state", entry.getKey().toString().toLowerCase())).metric(entry.getValue()).send();
        }

        for(Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : c.vm.garbageCollectors().entrySet()) {
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
