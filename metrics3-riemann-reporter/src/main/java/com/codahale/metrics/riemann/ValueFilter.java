package com.codahale.metrics.riemann;

/**
 * Filters values to determine if they fall into a given range via the
 * {@link #applies(double)} method. Ranges are intervals, with endpoint
 * inclusion configurable. The {@code state} property implicitly binds values in
 * the represented range to that state for reporting.
 * <p>
 * Instances are created using {@link ValueFilter.Builder}. Endpoints not
 * specified default to positive or negative infinity, respectively. For
 * example, {@code new ValueFilter.Builder("critical").withLower(50).build())}
 * creates a ValueFilter instance that binds "critical" to all values greater
 * than or equal to 50 and {@code new ValueFilter.Builder("warn")
 *                          .withUpperExclusive(300)
 *                          .withLower(100).build())} creates an instance
 * binding "warn" to values greater than or equal to 100 and strictly less than
 * 300.
 */
public class ValueFilter {

    /** Lower bound of the interval */
    private final double lower;

    /** Upper bound of the interval */
    private final double upper;

    /** Whether or not the upper bound is included */
    private final boolean upperIncluded;

    /** Whether or not the lower bound is included */
    private final boolean lowerIncluded;

    /** State bound to values in this interval */
    private final String state;

    /** Create a ValueFilter instance using the given builder */
    private ValueFilter(Builder builder) {
        this.lower = builder.lower;
        this.upper = builder.upper;
        this.upperIncluded = builder.upperIncluded;
        this.lowerIncluded = builder.lowerIncluded;
        this.state = builder.state;
    }

    /**
     * Determine whether or not the given value falls within the range
     * associated with this ValueFilter.
     * 
     * @param value value to test
     * @return true iff value is within the target interval
     */
    public boolean applies(double value) {
        boolean ret = upperIncluded ? value <= upper : value < upper;
        return ret && (lowerIncluded ? lower <= value : lower < value);
    }

    /**
     * @return the state bound to values in this ValueFilter
     */
    public String getState() {
        return state;
    }

    /**
     * Builder for ValueFilter instances
     */
    public static class Builder {

        /** Lower bound for the interval */
        private double lower = Double.NEGATIVE_INFINITY;

        /** Upper bound for the interval */
        private double upper = Double.POSITIVE_INFINITY;

        /** Whether or not the upper bound is included in the interval */
        private boolean upperIncluded = true;

        /** Whether or not the lower bound is included in the interval */
        private boolean lowerIncluded = true;

        /** State bound to values in the interval */
        private final String state;

        /**
         * Create a new builder for ValueFilters bound to the given state
         * 
         * @param state state bound to values in the interval
         */
        public Builder(String state) {
            this.state = state;
        }

        /**
         * @param lower inclusive lower bound for the interval
         * @return Builder instance with lower set inclusively (i.e. lower bound
         *         is included in the interval)
         */
        public Builder withLower(double lower) {
            this.lower = lower;
            return this;
        }

        /**
         * @param upper inclusive upper bound for the interval
         * @return Builder instance with upper set inclusively (i.e. upper bound
         *         is included in the interval)
         */
        public Builder withUpper(double upper) {
            this.upper = upper;
            return this;
        }

        /**
         * @param lower exclusive lower bound for the interval
         * @return Builder instance with lower set exclusively (i.e. lower bound
         *         is not included in the interval)
         */
        public Builder withLowerExclusive(double lower) {
            this.lower = lower;
            this.lowerIncluded = false;
            return this;
        };

        /**
         * @param upper exclusive upper bound for the interval
         * @return Builder instance with upper set exclusively (i.e. upper bound
         *         is not included in the interval)
         */
        public Builder withUpperExclusive(double upper) {
            this.upper = upper;
            this.upperIncluded = false;
            return this;
        }

        /**
         * Build a ValueFilter using this Builder instance
         * 
         * @return ValueFilter with properties determined by those of this
         *         Builder
         */
        public ValueFilter build() {
            return new ValueFilter(this);
        }
    }
}
