package com.aphyr.riemann.client;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class Promise<T> {
  public final CountDownLatch latch = new CountDownLatch(1);
  public final AtomicReference ref = new AtomicReference(latch);

  public Promise() {
  }

  public void deliver(T value) {
    if (0 < latch.getCount() && ref.compareAndSet(latch, value)) {
      latch.countDown();
    }
  }

  public void deliver(RuntimeException e) {
    if (0 < latch.getCount() && ref.compareAndSet(latch, e)) {
      latch.countDown();
    }
  }

  public T await() {
    try {
      latch.await();
      final Object value = ref.get();
      if (value instanceof RuntimeException) {
        throw (RuntimeException) value;
      } else {
        return (T) value;
      }
    } catch (InterruptedException e) {
      return null;
    }
  }

  public T await(long time, TimeUnit unit) {
    return await(time, unit, null);
  }

  public T await(long time, TimeUnit unit, T timeoutValue) {
    try {
      if (latch.await(time, unit)) {
        final Object value = ref.get();
        if (value instanceof RuntimeException) {
          throw (RuntimeException) value;
        } else {
          return (T) value;
        }
      } else {
        return timeoutValue;
      }
    } catch (InterruptedException e) {
      return timeoutValue;
    }
  }
}
