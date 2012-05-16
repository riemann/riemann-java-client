package com.aphyr.riemann.client;

import com.aphyr.riemann.Proto.Event;

import java.util.Arrays;
import java.util.List;
import java.io.IOException;

public class EventDSL {
    public AbstractRiemannClient client;
    public Event.Builder builder;

    public EventDSL(AbstractRiemannClient client) {
        this.client = client;
        this.builder = Event.newBuilder();
        try {
            this.builder.setHost(java.net.InetAddress.getLocalHost().getHostName());
        } catch (java.net.UnknownHostException e) {
            // If we can't get the local host, a null host is perfectly acceptable.
            // Caller will know soon enough. :)
        }
    }

    public EventDSL host(String host) {
        if (null == host) {
            builder.clearHost();
        } else {
            builder.setHost(host);
        }
        return this;
    }

    public EventDSL service(String service) {
        if (null == service) {
            builder.clearService();
        } else {
            builder.setService(service);
        }
        return this;
    }

    public EventDSL state(String state) {
        if (null == state) {
            builder.clearState();
        } else {
            builder.setState(state);
        }
        return this;
    }

    public EventDSL description(String description) {
        if (null == description) {
            builder.clearDescription();
        } else {
            builder.setDescription(description);
        }
        return this;
    }

    public EventDSL time(Null n) {
        builder.clearMetricF();
        return this;
    }

    public EventDSL time(float time) {
        builder.setTime((long) time);
        return this;
    }

    public EventDSL time(double time) {
        builder.setTime((long) time);
        return this;
    }
    public EventDSL time(long time) {
        builder.setTime(time);
        return this;
    }

    public EventDSL metric(Null n) {
        builder.clearMetricF();
        return this;
    }

    public EventDSL metric(float metric) {
        builder.setMetricF(metric);
        return this;
    }

    public EventDSL metric(int metric) {
        builder.setMetricF((float) metric);
        return this;
    }

    public EventDSL metric(long metric) {
        builder.setMetricF((float) metric);
        return this;
    }

    public EventDSL metric(double metric) {
        builder.setMetricF((float) metric);
        return this;
    }

    public EventDSL tag(String tag) {
        builder.addTags(tag);
        return this;
    }

    public EventDSL tags(List<String> tags) {
        builder.addAllTags(tags);
        return this;
    }

    public EventDSL tags(String... tags) {
        builder.addAllTags(Arrays.asList(tags));
        return this;
    }

    public EventDSL ttl(Null n) {
        builder.clearTtl();
        return this;
    }

    public EventDSL ttl(float ttl) {
        builder.setTtl(ttl);
        return this;
    }

    public Boolean sendWithAck() throws IOException, ServerError, MsgTooLargeException  {
        return client.sendEventsWithAck(builder.build());
    }

    public void send() {
        client.sendEvents(builder.build());
    }
}
