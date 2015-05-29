package com.aphyr.riemann.client;

import java.net.InetSocketAddress;

public class Resolver {
  protected InetSocketAddress remote;

  public Resolver(InetSocketAddress remote) {
    this.remote = remote;
  }

  public InetSocketAddress resolve() {
    return new InetSocketAddress(this.remote.getHostString(), this.remote.getPort());
  }
}
