package io.riemann.riemann.client;

import io.riemann.riemann.Proto;
import io.riemann.riemann.Proto.Attribute;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventBuilder {
    private final Proto.Event.Builder builder;
    private final Map<String, String> attributes = new HashMap<String, String>();

    public EventBuilder() {
        this.builder = Proto.Event.newBuilder();
    }

    public EventBuilder host(String host) {
        if (null == host) {
            builder.clearHost();
        } else {
            builder.setHost(host);
        }
        return this;
    }

    public EventBuilder service(String service) {
        if (null == service) {
            builder.clearService();
        } else {
            builder.setService(service);
        }
        return this;
    }

    public EventBuilder state(String state) {
        if (null == state) {
            builder.clearState();
        } else {
            builder.setState(state);
        }
        return this;
    }

    public EventBuilder description(String description) {
        if (null == description) {
            builder.clearDescription();
        } else {
            builder.setDescription(description);
        }
        return this;
    }

    public EventBuilder time() {
        builder.clearMetricF();
        return this;
    }

    public EventBuilder time(float time) {
        builder.setTime((long) time);
        builder.setTimeMicros((long) (time * 1000000));
        return this;
    }

    public EventBuilder time(double time) {
        builder.setTime((long) time);
        builder.setTimeMicros((long) (time * 1000000));
        return this;
    }

    public EventBuilder time(long time) {
        builder.setTime(time);
        return this;
    }

    public EventBuilder metric() {
        builder.clearMetricF();
        builder.clearMetricD();
        builder.clearMetricSint64();
        return this;
    }

    public EventBuilder metric(byte metric) {
        builder.setMetricSint64((long) metric);
        builder.setMetricF((float) metric);
        return this;
    }

    public EventBuilder metric(short metric) {
        builder.setMetricSint64((long) metric);
        builder.setMetricF((float) metric);
        return this;
    }

    public EventBuilder metric(int metric) {
        builder.setMetricSint64((long) metric);
        builder.setMetricF((float) metric);
        return this;
    }

    public EventBuilder metric(long metric) {
        builder.setMetricSint64(metric);
        builder.setMetricF((float) metric);
        return this;
    }

    public EventBuilder metric(float metric) {
        builder.setMetricF(metric);
        return this;
    }

    public EventBuilder metric(double metric) {
        builder.setMetricD(metric);
        builder.setMetricF((float) metric);
        return this;
    }

    public EventBuilder tag(String tag) {
        builder.addTags(tag);
        return this;
    }

    public EventBuilder tags(List<String> tags) {
        builder.addAllTags(tags);
        return this;
    }

    public EventBuilder tags(String... tags) {
        builder.addAllTags(Arrays.asList(tags));
        return this;
    }

    public EventBuilder ttl() {
        builder.clearTtl();
        return this;
    }

    public EventBuilder ttl(float ttl) {
        builder.setTtl(ttl);
        return this;
    }

    public EventBuilder attribute(String name, String value) {
        attributes.put(name, value);
        return this;
    }

    public EventBuilder attributes(Map<String, String> attributes) {
        this.attributes.putAll(attributes);
        return this;
    }

    // Returns the compiled Protobuf event for this DSL. Merges in the custom
    // attributes map. Can only be called safely once.
    public Proto.Event build() {
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            Attribute.Builder attribBuilder = Attribute.newBuilder();
            attribBuilder.setKey(entry.getKey());
            attribBuilder.setValue(entry.getValue());
            builder.addAttributes(attribBuilder);
        }
        return builder.build();
    }
}
