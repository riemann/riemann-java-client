package com.aphyr.riemann.client;

import java.util.concurrent.TimeUnit;
import java.io.IOException;
import clojure.lang.IDeref;
import clojure.lang.IBlockingDeref;

public interface IPromise<T> extends IDeref, IBlockingDeref {
  // Fulfill this promise with a single value. Only the first call to
  // deliver() will succeed; successive deliveries are noops.
  public void deliver(final Object value);

  // Dereference this promise, blocking indefinitely.
  public T deref() throws IOException;

  // Dereference this promise, returning null if it times out.
  public T deref(final long time, final TimeUnit unit) throws IOException;

  // Dereference this promise, returning a T.
  public T deref(final long time,
                 final TimeUnit unit,
                 final T timeoutValue) throws IOException;

  // A timed deref that allows any object for the timeout value. You and I
  // both know this will return either a T or just timeoutValue, but
  // unfortunately, as you probably already know, the Java type system.
  public Object unsafeDeref(final long time,
                            final TimeUnit unit,
                            final Object timeoutValue) throws IOException;

  // Return a new promise, based on this one, which converts values from T to
  // T2 using a function.
  public <T2> IPromise<T2> map(Fn2<T, T2> f);
}
