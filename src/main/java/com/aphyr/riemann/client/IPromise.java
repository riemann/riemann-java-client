package com.aphyr.riemann.client;

import java.util.concurrent.TimeUnit;
import java.io.IOException;
import clojure.lang.IDeref;

public interface IPromise<T> extends IDeref {
  public void deliver(Object value);
  public T deref() throws IOException;
  public T deref(long time, TimeUnit unit) throws IOException;
  public T deref(long time, TimeUnit unit, T timeoutValue) throws IOException;
}
