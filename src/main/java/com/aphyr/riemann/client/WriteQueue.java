package com.aphyr.riemann.client;

import com.aphyr.riemann.Proto.Msg;
import java.io.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

// A synchronized FIFO queue intended to track the outstanding writes
// associated with a TCP connection. Calling close() flushes the queue
// atomically and prevents future writes. Open() allows writes to be enqueued
// again.
public class WriteQueue {
  public boolean isOpen = true;
  public volatile int size = 0;
  public final LinkedBlockingQueue<Promise<Msg>> queue =
    new LinkedBlockingQueue<Promise<Msg>>();

  public synchronized void open() {
    isOpen = true;
    size = 0;
  }

  // Returns the number of promises cleared.
  public synchronized void close(Throwable t) {
    isOpen = false;

    // Deliver exceptions to all outstanding promises.
    final IOException ex = new IOException("channel closed", t);
    Promise<Msg> promise;
    while ((promise = queue.poll()) != null) {
      promise.deliver(ex);
    }

    size = 0;
  }

  public int size() {
    return this.size;
  }

  public synchronized void put(final Promise<Msg> p) throws InterruptedException {
    if (isOpen) {
      try {
        queue.put(p);
        size++;
      } catch (RuntimeException e) {
        size = queue.size();
        throw e;
      } catch (InterruptedException e) {
        size = queue.size();
        throw e;
      }
    } else {
      p.deliver(new IOException("Channel closed."));
    }
  }

  public synchronized Promise<Msg> take() {
    try {
      final Promise<Msg> p = queue.take();
      size--;
      return p;
    } catch (RuntimeException e) {
      size = queue.size();
      throw e;
    } catch (InterruptedException e) {
      size = queue.size();
      throw new RuntimeException(e);
    }
  }
}
