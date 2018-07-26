package com.codahale.metrics.riemann;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class ValueFilterMap {

    // Message constants
    public static final String MAX = "max";

    public static final String MEAN = "mean";

    public static final String MIN = "min";

    public static final String STDDEV = "stddev";

    public static final String P50 = "p50";

    public static final String P75 = "p75";

    public static final String P95 = "p95";

    public static final String P98 = "p98";

    public static final String P99 = "p99";

    public static final String P999 = "p999";

    public static final String COUNT = "count";

    public static final String M1_RATE = "m1_rate";

    public static final String M5_RATE = "m5_rate";

    public static final String M15_RATE = "m15_rate";

    public static final String MEAN_RATE = "mean_rate";

    // TODO: when Java 8 is available, replace this bloated setup with lambdas
    //
    /** Implementations extract specific values from snapshots */
    interface SnapValue {

        /** @return the snapshot value that this lamba-like thing is keyed on */
        double value(Snapshot snapshot);
    }

    /** Implementations extract specific statistics from metered impls */
    interface MeteredValue {

        /** @return the metered value that this lamba-like thing is keyed on */
        double value(Metered metered);
    }

    /**
     * Map with keys equal to snapshot statistics names and values lambda-like
     * objects that extract the named statistic - statically initialized
     */
    private static final Map<String, SnapValue> snapValueMap = new HashMap<String, SnapValue>();

    /**
     * Map with keys equal to metered value names and values lambda-like objects
     * that extract the named metrics - statically initialized
     */
    private static final Map<String, MeteredValue> meteredValueMap = new HashMap<String, MeteredValue>();

    /** Loads lambda-like maps used to extract values from metrics */
    static {
        snapValueMap.put(MAX, new SnapValue() {

            public double value(Snapshot snapshot) {
                return snapshot.getMax();
            }
        });
        snapValueMap.put(MEAN, new SnapValue() {

            public double value(Snapshot snapshot) {
                return snapshot.getMean();
            }
        });
        snapValueMap.put(MIN, new SnapValue() {

            public double value(Snapshot snapshot) {
                return snapshot.getMin();
            }
        });
        snapValueMap.put(STDDEV, new SnapValue() {

            public double value(Snapshot snapshot) {
                return snapshot.getStdDev();
            }
        });
        snapValueMap.put(P50, new SnapValue() {

            public double value(Snapshot snapshot) {
                return snapshot.getMedian();
            }
        });
        snapValueMap.put(P75, new SnapValue() {

            public double value(Snapshot snapshot) {
                return snapshot.get75thPercentile();
            }
        });
        snapValueMap.put(P95, new SnapValue() {

            public double value(Snapshot snapshot) {
                return snapshot.get95thPercentile();
            }
        });
        snapValueMap.put(P98, new SnapValue() {

            public double value(Snapshot snapshot) {
                return snapshot.get98thPercentile();
            }
        });
        snapValueMap.put(P99, new SnapValue() {

            public double value(Snapshot snapshot) {
                return snapshot.get99thPercentile();
            }
        });
        snapValueMap.put(P999, new SnapValue() {

            public double value(Snapshot snapshot) {
                return snapshot.get999thPercentile();
            }
        });

        meteredValueMap.put(COUNT, new MeteredValue() {

            public double value(Metered metered) {
                return metered.getCount();
            }
        });
        meteredValueMap.put(M1_RATE, new MeteredValue() {

            public double value(Metered metered) {
                return metered.getOneMinuteRate();
            }
        });
        meteredValueMap.put(M5_RATE, new MeteredValue() {

            public double value(Metered metered) {
                return metered.getFiveMinuteRate();
            }
        });
        meteredValueMap.put(M15_RATE, new MeteredValue() {

            public double value(Metered metered) {
                return metered.getFifteenMinuteRate();
            }
        });
        meteredValueMap.put(MEAN_RATE, new MeteredValue() {

            public double value(Metered metered) {
                return metered.getMeanRate();
            }
        });
    }

    /**
     * If m is a Metric, then filterMapMap.get(m) is a map with keys equal to
     * measures (constants above) and values equal to ValueFilter instances.
     * ValueFilters mapped to measures are applied in determining the state
     * associated with the given measure reported to Riemann in m's report.
     */
    private final Map<Metric, Map<String, List<ValueFilter>>> filterMapMap = new ConcurrentHashMap<Metric, Map<String, List<ValueFilter>>>();

    public ValueFilterMap() {
        super();
    }

    public ValueFilterMap add(Metric metric, String measure,
                              ValueFilter filter) {
        Map<String, List<ValueFilter>> filterMap = filterMapMap.get(metric);
        List<ValueFilter> filterList;
        if (filterMap == null) {
            filterMap = new HashMap<String, List<ValueFilter>>();
            filterMapMap.put(metric, filterMap);
        }
        filterList = filterMap.get(measure);
        if (filterList == null) {
            filterList = new ArrayList<ValueFilter>();
            filterMap.put(measure, filterList);
        }
        filterList.add(filter);
        return this;
    }

    public List<ValueFilter> get(Metric metric, String measure) {
        final Map<String, List<ValueFilter>> filterMap = filterMapMap
            .get(metric);
        if (filterMap == null) {
            return null;
        }
        return filterMap.get(measure);
    }

    public String state(Timer timer, String measure) {
        final Snapshot snap = timer.getSnapshot();
        final double value = snapValueMap.get(measure).value(snap);
        return state(get(timer, measure), value);
    }

    public String state(Histogram histogram, String measure) {
        double value;
        if (measure.equals(COUNT)) {
            value = histogram.getCount();
        } else {
            final Snapshot snap = histogram.getSnapshot();
            value = snapValueMap.get(measure).value(snap);
        }
        return state(get(histogram, measure), value);
    }

    public String state(Metered metered, String measure) {
        final double value = meteredValueMap.get(measure).value(metered);
        return state(get(metered, measure), value);
    }

    public String state(Counter counter) {
        final double value = counter.getCount();
        return state(get(counter, "count"), value);
    }

    public synchronized void clear(Metric metric) {
        final Map<String, List<ValueFilter>> filters = filterMapMap.get(metric);
        for (Entry<String, List<ValueFilter>> entry : filters.entrySet()) {
            entry.getValue().clear();
        }
        filters.clear();
    }

    public synchronized void clear() {
        for (Metric metric : filterMapMap.keySet()) {
            clear(metric);
        }
    }

    private String state(List<ValueFilter> filters, double value) {
        String ret = "ok";
        if (filters != null) {
            for (ValueFilter filter : filters) {
                if (filter.applies(value)) {
                    ret = filter.getState();
                }
            }
        }
        return ret;
    }
}
