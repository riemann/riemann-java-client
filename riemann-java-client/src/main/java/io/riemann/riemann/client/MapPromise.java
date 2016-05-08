package io.riemann.riemann.client;

import java.util.concurrent.TimeUnit;
import java.io.IOException;

// Maps a Promise of type T1 to a promise of type T2 by applying a function
// T1->T2 on a call to deref(). Caches the value so f is only invoked once (or
// zero times if a timeout/error occurs).
public class MapPromise<T1, T2> implements IPromise<T2> {
  public static final Object sentinel = new Object();

  public volatile Object value = sentinel;
  public final IPromise<T1> p;
  public final Fn2<T1, T2> f;

  // Wrap a promise of type T1 with a function from T1->T2.
  public MapPromise(final IPromise<T1> p, final Fn2<T1, T2> f) {
    this.p = p;
    this.f = f;
  }

  // Call map and trap RuntimeExceptions as values
  // Fuck me, I miss macros
  public Object mapCapturingExceptions(final T1 x) {
    try {
      return f.call(x);
    } catch (RuntimeException e) {
      return e;
    }
  }

  // Delivering to a map promise delivers to the underlying promise.
  public void deliver(final Object value) {
    p.deliver(value);
  }

  // Deref, applying map to the value returned by the wrapped promise.
  public T2 deref() throws IOException {
    if (sentinel == value) {
      synchronized(this) {
        if (sentinel == value) {
          value = mapCapturingExceptions(p.deref());
        }
      }
    }
    return (T2) Promise.rehydrate(value);
  }

  public T2 deref(final long time, final TimeUnit unit) throws IOException {
    return deref(time, unit, null);
  }

  public T2 deref(final long time,
                  final TimeUnit unit,
                  final T2 timeoutValue) throws IOException {
    return (T2) unsafeDeref(time, unit, timeoutValue);
  }

  @Override
  public Object deref(final long millis, final Object timeoutValue)
                     throws IOException {
    return unsafeDeref(millis, TimeUnit.MILLISECONDS, timeoutValue);
  }

  public Object unsafeDeref(final long time,
                            final TimeUnit unit,
                            final Object timeoutValue) throws IOException {
    if (sentinel == value) {
      synchronized(this) {
        if (sentinel == value) {
          final Object response = p.unsafeDeref(time, unit, sentinel);
          if (sentinel == response) {
            return timeoutValue;
          } else {
            // If we *didn't* get our timeout value, unsafeDeref is guaranteed
            // to give us a T1.
            value = mapCapturingExceptions((T1) response);
          }
        }
      }
    }
    return (T2) Promise.rehydrate(value);
  }

  @Override
  public <T3> IPromise<T3> map(Fn2<T2, T3> f) {
    return new MapPromise<T2, T3>(this, f);
  }
}
