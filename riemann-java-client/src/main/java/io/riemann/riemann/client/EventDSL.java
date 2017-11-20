package io.riemann.riemann.client;

import io.riemann.riemann.Proto.Attribute;
import io.riemann.riemann.Proto.Event;
import io.riemann.riemann.Proto.Msg;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class EventDSL {
    public final IRiemannClient client;
    public final EventBuilder event;

    public EventDSL(IRiemannClient client) {
        this.client = client;
        this.event = new EventBuilder();
        try {
            this.event.host(java.net.InetAddress.getLocalHost().getHostName());
        } catch (java.net.UnknownHostException e) {
            // If we can't get the local host, a null host is perfectly
            // acceptable.  Caller will know soon enough. :)
        }
    }

    public EventDSL host(String host) {
        this.event.host(host);
        return this;
    }

    public EventDSL service(String service) {
        this.event.service(service);
        return this;
    }

    public EventDSL state(String state) {
        this.event.state(state);
        return this;
    }

    public EventDSL description(String description) {
        this.event.description(description);
        return this;
    }

    public EventDSL time(Null n) {
        this.event.time();
        return this;
    }

    public EventDSL time(float time) {
        this.event.time(time);
        return this;
    }

    public EventDSL time(double time) {
        this.event.time(time);
        return this;
    }
    public EventDSL time(long time) {
        this.event.time(time);
        return this;
    }

    public EventDSL metric(Null n) {
        this.event.metric();
        return this;
    }

    public EventDSL metric(byte metric) {
        this.event.metric(metric);
        return this;
    }

    public EventDSL metric(short metric) {
        this.event.metric(metric);
        return this;
    }

    public EventDSL metric(int metric) {
        this.event.metric(metric);
        return this;
    }

    public EventDSL metric(long metric) {
        this.event.metric(metric);
        return this;
    }

    public EventDSL metric(float metric) {
        this.event.metric(metric);
        return this;
    }

    public EventDSL metric(double metric) {
        this.event.metric(metric);
        return this;
    }

    public EventDSL tag(String tag) {
        this.event.tag(tag);
        return this;
    }

    public EventDSL tags(List<String> tags) {
        this.event.tags(tags);
        return this;
    }

    public EventDSL tags(String... tags) {
        this.event.tags(tags);
        return this;
    }

    public EventDSL ttl(Null n) {
        this.event.ttl();
        return this;
    }

    public EventDSL ttl(float ttl) {
        this.event.ttl(ttl);
        return this;
    }

    public EventDSL attribute(String name, String value) {
        this.event.attribute(name, value);
        return this;
    }

    public EventDSL attributes(Map<String, String> attributes) {
        this.event.attributes(attributes);
        return this;
    }

    // Returns the compiled Protobuf event for this DSL. Merges in the custom
    // attributes map. Can only be called safely once.
    public Event build() {
        return this.event.build();
    }

    public IPromise<Msg> send() {
      return client.sendEvent(build());
    }
}
