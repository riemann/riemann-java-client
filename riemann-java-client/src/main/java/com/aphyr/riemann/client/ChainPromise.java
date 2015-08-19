package com.aphyr.riemann.client;

import java.util.concurrent.TimeUnit;
import java.io.IOException;

// Imagine you're calling a function send() which returns an
// IPromise<Response>, and want to return that promise to the caller.
//
// IPromise<Response> cleanHouse() {
//   return send();
// }
//
// Problem is, you're not going to call send(); yet.
//
// IPromise<Response> cleanHouse() {
//   meh(sureDadWhatever);
//
//   return // um what goes here???
// }
//
// You're gonna call it *later*, because you're doing some kind of batching
// async magic. When you *do* call send() you'll be able to hand that result
// off to the client, but not yet.
//
// public void eventually() {
//   final IPromise<Response> = send();
//   // OK now what?
// }
//
// Solution: construct a ChainPromise<Response> *now*, and call .attach(send())
// on that ChainPromise *later*, which hooks the ChainPromise up to the
// response of send(). Derefs on the ChainPromise wait until .attach() happens,
// then dereference *that*.
//
// JUST ADD MORE LATCHES AND ATOMICREFERENCES, EVERYTHING IS FUCKING FINE
public class ChainPromise<T> implements IPromise<T> {
  public final IPromise<IPromise<T>> inner = new Promise<IPromise<T>>();

  public void attach(final IPromise<T> innerValue) {
    inner.deliver(innerValue);
  }

  @Override
  public void deliver(final Object value) {
    throw new UnsupportedOperationException("Can't deliver to a chained promise.; deliver to the underlying promise instead?");
  }

  @Override
  public T deref() throws IOException {
    return inner.deref().deref();
  }

  @Override
  public T deref(final long time, final TimeUnit unit) throws IOException {
    return deref(time, unit, null);
  }

  @Override
  public T deref(final long time, final TimeUnit unit, final T timeoutValue)
                throws IOException {
    return (T) unsafeDeref(time, unit, timeoutValue);
  }

  @Override
  public Object deref(final long millis, final Object timeoutValue)
                     throws IOException {
    return unsafeDeref(millis, TimeUnit.MILLISECONDS, timeoutValue);
  }

  @Override
  public Object unsafeDeref(final long time,
                            final TimeUnit unit,
                            final Object timeoutValue)
                           throws IOException {
    final long t1 = System.nanoTime();

    // Extract our attached promise
    final Object attached = inner.unsafeDeref(time, unit, timeoutValue);
    if (timeoutValue == attached) {
      return timeoutValue;
    }

    // How much time left?
    final long remainingNanos = t1 + unit.toNanos(time) - System.nanoTime();

    // Since inner.unsafeDeref did *not* return the timeoutValue, it gave us an
    // IPromise<T>.
    return ((IPromise<T>) attached)
      .unsafeDeref(remainingNanos, TimeUnit.NANOSECONDS, timeoutValue);
  }

  @Override
  public <T2> IPromise<T2> map(Fn2<T, T2> f) {
    return new MapPromise<T, T2>(this, f);
  }
}
