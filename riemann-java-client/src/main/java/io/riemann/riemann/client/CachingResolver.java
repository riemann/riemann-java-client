package io.riemann.riemann.client;

import java.net.InetSocketAddress;

public class CachingResolver extends Resolver {
  public CachingResolver(InetSocketAddress remote) {
    super(remote);
  }

  public InetSocketAddress resolve() {
    return this.remote;
  }
}
