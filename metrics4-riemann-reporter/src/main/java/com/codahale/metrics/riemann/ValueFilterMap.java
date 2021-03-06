package com.codahale.metrics.riemann;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * Enables {@link ValueFilter}s to be associated with measures reported by
 * Metrics so that state can be computed based on values at report-generation
 * time. The {@link #addFilter(Metric, String, ValueFilter)} method associates a
 * {@link ValueFilter} with a measure and the {@code state} methods compute what
 * state should be reported based on the value of a measure.
 * <p>
 * For example, {@code
 *   ValueFilterMap valueFilterMap = new ValueFilterMap();
 *   valueFilterMap
            .addFilter(timer, ValueFilterMap.MAX,
                 new ValueFilter.Builder("critical").withLower(50).build())
            .addFilter(timer, ValueFilterMap.MEAN, new ValueFilter.Builder("warn")
                .withUpperExclusive(200).withLower(100).build());
 * } Attaches filters to the mean and max values reported by timers so that
 * {@code state(timer, "max")} will return "critical" if the max value reported
 * by the timer is greater than or equal to 50 and {@code state(timer, "mean")}
 * will return "warn" if the mean value is between 100 (inclusive) and 200
 * (exclusive). Filters are applied in the order they are added and the last one
 * that applies wins. If no filter applies, state methods return "ok".
 */
public class ValueFilterMap {

    // Names for values reported by metrics - call these "measures"
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

    /**
     * Loads lambda-like functions into maps used to extract values from metrics
     */
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
     * ValueFilters mapped to measures are applied to determine the state
     * associated with the given measure reported to Riemann in m's report. For
     * example, if t is a timer, filterMapMap(t) is a map of lists of filters.
     * The keys to that map are "max", "mean", "min", etc. - all of the measures
     * that t has getters for. The associated value is a list of filters. The
     * last filter that applies determines what the reported state is for the
     * measure.
     */
    private final Map<Metric, Map<String, List<ValueFilter>>> filterMapMap = new ConcurrentHashMap<Metric, Map<String, List<ValueFilter>>>();

    public ValueFilterMap() {
        super();
    }

    /**
     * Add a filter associated with the given measure for the given metric.
     *
     * @param metric metric instance reporting the measure that the filter
     *        applies to
     * @param measure name of the value reported by the metric that the filter
     *        applies to
     * @param filter ValueFilter instance that, if it applies, may determine the
     *        state reported for the measure
     * @return a reference to *this (for fluent activation)
     */
    public ValueFilterMap addFilter(Metric metric, String measure,
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

    /**
     * Returns the list of filters that have been added to the given metric,
     * measure pair.
     *
     * @param metric metric the measure belongs to
     * @param measure name of one of the values reported by the metric
     * @return list of filters that should be applied when determining the state
     *         reported with the given measure
     */
    public List<ValueFilter> getFilterList(Metric metric, String measure) {
        final Map<String, List<ValueFilter>> filterMap = filterMapMap
            .get(metric);
        if (filterMap == null) {
            return null;
        }
        return filterMap.get(measure);
    }

    /**
     * Determines the state that should be reported with the given measure for
     * the given timer instance.
     *
     * @param timer timer instance
     * @param measure name of the value reported by the timer whose reported
     *        state is sought
     * @param durationUnit TimeUnit to convert timer's native (nanosecond)
     *        measurements to
     * @return state corresponding to the given measure
     */
    public String state(Timer timer, String measure, TimeUnit durationUnit) {
        final Snapshot snap = timer.getSnapshot();
        final double value = snapValueMap.get(measure).value(snap) /
                             durationUnit.toNanos(1);
        return state(getFilterList(timer, measure), value);
    }

    /**
     * Return the state that should be reported for the given measure, histogram
     * pair.
     * 
     * @param histogram histogram instance
     * @param measure name of a measure included in the histogram report
     * @return the state associated to the current value of the measure in the
     *         histogram, or "ok" if there is no state mapped to this measure,
     *         histogram pair.
     */
    public String state(Histogram histogram, String measure) {
        double value;
        if (measure.equals(COUNT)) {
            value = histogram.getCount();
        } else {
            final Snapshot snap = histogram.getSnapshot();
            value = snapValueMap.get(measure).value(snap);
        }
        return state(getFilterList(histogram, measure), value);
    }

    /**
     * Return the state that should be reported for the given measure, metered
     * instance pair.
     * 
     * @param metered metered instance
     * @param measure name of a measure included in the metered instance report
     * @return the state associated to the current value of the measure in the
     *         metered instance, or "ok" if there is no state mapped to this
     *         measure, metered instance pair.
     */
    public String state(Metered metered, String measure) {
        final double value = meteredValueMap.get(measure).value(metered);
        return state(getFilterList(metered, measure), value);
    }

    /**
     * Return the state that should be reported for the given counter, based on
     * its value.
     * 
     * @param counter Counter instance
     * @return the state associated to the current value of the counter in the
     *         or "ok" if there is no state mapped to this Counter value.
     */
    public String state(Counter counter) {
        final double value = counter.getCount();
        return state(getFilterList(counter, "count"), value);
    }

    /**
     * Clear all state mappings associated with the given Metric.
     * 
     * @param metric Metric instance to clear mappings for
     */
    public synchronized void clear(Metric metric) {
        final Map<String, List<ValueFilter>> filters = filterMapMap.get(metric);
        for (Entry<String, List<ValueFilter>> entry : filters.entrySet()) {
            entry.getValue().clear();
        }
        filters.clear();
    }

    /**
     * Clear all mappings for all Metric instances.
     */
    public synchronized void clear() {
        for (Metric metric : filterMapMap.keySet()) {
            clear(metric);
        }
    }

    /**
     * Test all ValueFilters in filters against the given value. If none apply,
     * return "ok"; otherwise return the state associated with the last filter
     * that applies.
     * 
     * @param filters List of ValueFilters to test
     * @param value value to test
     * @return "ok" or the state associated with the last filter in the list
     *         that applies
     */
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
