package com.aphyr.riemann.client;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.io.IOException;

public class Promise<T> implements IPromise<T> {
  // We store exceptions and actual values in the same reference; on deref(),
  // we have to *throw*, not return, the exceptions. This fun takes an Object
  // which is either an IOException, a RuntimeException, or a T. Throws the
  // exceptions, returns a T.
  public static Object rehydrate(final Object value) throws IOException {
    if (value instanceof IOException) {
      throw (IOException) value;
    } else if (value instanceof RuntimeException) {
      throw (RuntimeException) value;
    } else {
      return value;
    }
  }

  public static Object asResult(Object value) {
    if (value instanceof Exception) {
      return null;
    }
    else {
      return value;
    }
  }

  public static Exception asException(Object value){
    if (value instanceof Exception) {
      return (Exception) value;
    }
    else {
      return null;
    }
  }

  public final CountDownLatch latch = new CountDownLatch(1);
  public final AtomicReference ref = new AtomicReference(latch);

  private Callback<T> callback;

  public Promise() { }

  public void deliver(Object value) {
    if (0 < latch.getCount() && ref.compareAndSet(latch, value)) {
      latch.countDown();
      if (callback != null) {
        callback.call((T) asResult(value), asException(value));
      }
    }
  }

  public T deref() throws IOException {
    try {
      latch.await();
      return (T) rehydrate(ref.get());
    } catch (InterruptedException e) {
      return null;
    }
  }

  public T deref(final long time, final TimeUnit unit) throws IOException {
    return deref(time, unit, null);
  }

  // Type-safe deref
  public T deref(final long time,
                 final TimeUnit unit,
                 final T timeoutValue) throws IOException {
    // My kingdom for a union type
    return (T) unsafeDeref(time, unit, timeoutValue);
  }

  @Override
  public Object deref(final long millis, final Object timeoutValue)
                     throws IOException {
    return unsafeDeref(millis, TimeUnit.MILLISECONDS, timeoutValue);
  }

  // A timed deref that allows any object for the timeout value. You and I
  // both know this will return either a T1 or just timeoutValue, but
  // unfortunately, as you probably already know, the Java type system.
  public Object unsafeDeref(final long time,
                            final TimeUnit unit,
                            final Object timeoutValue) throws IOException {
    try {
      if (latch.await(time, unit)) {
        return (T) rehydrate(ref.get());
      } else {
        return timeoutValue;
      }
    } catch (InterruptedException e) {
      return timeoutValue;
    }
  }

  @Override
  public void setCallback(Callback<T> c) {
    callback = c;
  }

  // Return a new promise, based on this one, which converts values from T to
  // T2 using a function.
  @Override
  public <T2> IPromise<T2> map(Fn2<T, T2> f) {
    return new MapPromise<T, T2>(this, f);
  }
}
