package com.codahale.metrics.riemann;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import com.yammer.metrics.core.VirtualMachineMetrics;

import java.util.*;
import java.util.concurrent.TimeUnit;

/*
 * com.yammer.metrics.reporting.RiemannReporter style vmmetrics for backwards
 * compatability.
 *
 * Use of the JVM metric sets provided by codahale metrics 3 over this metric
 * set is highly recommended.
 *
 */
public class JvmMetricSet implements MetricSet {

    final private String separator;
    final private VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();

    public JvmMetricSet() {
        this(" ");
    }

    public JvmMetricSet(String separator) {
        this.separator = separator;
    }

    private String service(String... parts) {
        final StringBuilder sb = new StringBuilder();
        for (String p : parts) {
            sb.append(p).append(this.separator);
        }

        return sb.substring(0, sb.length() - this.separator.length());
    }

    @Override
    public Map<String, Metric> getMetrics() {
        final Map<String, Metric> gauges = new HashMap<String, Metric>();

        gauges.put(service("jvm", "memory", "heap-usage"), new Gauge<Double>() {
            @Override
            public Double getValue() {
                return vm.heapUsage();
            }
        });
        gauges.put(service("jvm", "memory", "non-heap-usage"), new Gauge<Double>() {
            @Override
            public Double getValue() {
                return vm.nonHeapUsage();
            }
        });
        gauges.put(service("jvm", "thread", "daemon-count"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return vm.daemonThreadCount();
            }
        });
        gauges.put(service("jvm", "thread", "count"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return vm.threadCount();
            }
        });
        gauges.put(service("jvm", "uptime"), new Gauge<Long>() {
            @Override
            public Long getValue() {
                return vm.uptime();
            }
        });
        gauges.put(service("jvm", "fd-usage"), new Gauge<Double>() {
            @Override
            public Double getValue() {
                return vm.fileDescriptorUsage();
            }
        });


        for (final Map.Entry<String, Double> pool : this.vm.memoryPoolUsage().entrySet()) {
            gauges.put(service("jvm", "memory", "pool-usage", pool.getKey()), new Gauge<Double>() {
                @Override
                public Double getValue() {
                    return pool.getValue();
                }
            });
        }
        for(final Map.Entry<Thread.State, Double> entry : this.vm.threadStatePercentages().entrySet()) {
            gauges.put(service("jvm", "thread", "state", entry.getKey().toString().toLowerCase()), new Gauge<Double>() {
                @Override
                public Double getValue() {
                    return entry.getValue();
                }
            });
        }
        for(final Map.Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : this.vm.garbageCollectors().entrySet()) {
            gauges.put(service("jvm", "gc", entry.getKey(), "time"), new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return entry.getValue().getTime(TimeUnit.MILLISECONDS);
                }
            });

            gauges.put(service("jvm", "gc", entry.getKey(), "runs"), new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return entry.getValue().getRuns();
                }
            });
        }

        return Collections.unmodifiableMap(gauges);
    }
}
