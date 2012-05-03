package com.aphyr.riemann.client;

import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.client.RiemannClient;

import java.util.Arrays;
import java.util.List;
import java.io.IOException;

public class EventDSL {
  public RiemannClient client;
  public Event.Builder builder;

  public EventDSL(RiemannClient client) {
    this.client = client;
    this.builder = Event.newBuilder();
    try {
      this.builder.setHost(java.net.InetAddress.getLocalHost().getHostName());
    } catch (java.net.UnknownHostException e) {}
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
  
  public EventDSL time(Long time) {
    if (null == time) {
      builder.clearTime();
    } else {
      builder.setTime(time);
    }
    return this;
  }
  
  public EventDSL metric(Float metric) {
    if (null == metric) {
      builder.clearMetricF();
    } else {
      builder.setMetricF(metric);
    }
    return this;
  }

  public EventDSL metric(Integer metric) {
    if (null == metric) {
      builder.clearMetricF();
    } else {
      builder.setMetricF((float) metric);
    }
    return this;
  }
  
  public EventDSL tag(String tag) {
    builder.addTags(tag);
    return this;
  }
  
  public EventDSL ttl(Float ttl) {
    if (null == ttl) {
      builder.clearTtl();
    } else {
      builder.setTtl(ttl);
    }
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

  public Boolean sendWithAck() throws IOException, ServerError {
    return client.sendEventsWithAck(builder.build());
  }

  public void send() {
    client.sendEvents(builder.build());
  }
}
