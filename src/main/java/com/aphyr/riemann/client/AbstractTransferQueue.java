package com.aphyr.riemann.client;

import java.util.Collection;

public abstract class AbstractTransferQueue<E> {
  public abstract int drainTo(Collection<? super E> c);
  public abstract int drainTo(Collection<? super E> c, int maxElements);
  public abstract void put(E e);
}
