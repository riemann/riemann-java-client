package com.aphyr.riemann.client;

// A function from T1 to T2.
public interface Fn2<T1, T2> {
  public T2 call(final T1 x);
}
