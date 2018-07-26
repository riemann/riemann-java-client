package com.codahale.metrics.riemann;

public class ValueFilter {

    private final double lower;

    private final double upper;

    private final boolean upperIncluded;

    private final boolean lowerIncluded;

    private final String state;

    private ValueFilter(Builder builder) {
        this.lower = builder.lower;
        this.upper = builder.upper;
        this.upperIncluded = builder.upperIncluded;
        this.lowerIncluded = builder.lowerIncluded;
        this.state = builder.state;
    }

    public boolean applies(double value) {
        boolean ret = upperIncluded ? value <= upper : value < upper;
        return ret && lowerIncluded ? lower <= value : lower < value;
    }

    public String getState() {
        return state;
    }

    public static class Builder {

        private double lower = Double.MIN_VALUE;

        private Double upper = Double.MAX_VALUE;

        private boolean upperIncluded = true;

        private boolean lowerIncluded = true;

        private String state = "ok";

        public Builder(String state) {
            this.state = state;
        }

        public Builder withLower(double lower) {
            this.lower = lower;
            return this;
        }

        public Builder withUpper(double upper) {
            this.upper = upper;
            return this;
        }

        public Builder withLowerExclusive(double lower) {
            this.lower = lower;
            this.lowerIncluded = false;
            return this;
        };

        public Builder withUpperExclusive(double upper) {
            this.upper = upper;
            this.upperIncluded = false;
            return this;
        }

        public ValueFilter build() {
            return new ValueFilter(this);
        }
    }
}
