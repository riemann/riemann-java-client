package com.aphyr.riemann.client;

import com.aphyr.riemann.Proto.Msg;
import java.io.*;
import java.util.concurrent.LinkedBlockingQueue;

// A synchronized FIFO queue intended to track the outstanding writes
// associated with a TCP connection. Calling close() flushes the queue
// atomically and prevents future writes. Open() allows writes to be enqueued
// again.
public class WriteQueue {
  public boolean isOpen = true;
  public final LinkedBlockingQueue<Promise<Msg>> queue =
    new LinkedBlockingQueue<Promise<Msg>>();

  public synchronized void open() {
    isOpen = true;
  }

  public synchronized void close() {
    isOpen = false;
    
    // Deliver exceptions to all outstanding promises.
    final IOException ex = new IOException("Channel closed.");
    Promise<Msg> promise;
    while ((promise = queue.poll()) != null) {
      promise.deliver(ex);
    }
  }

  public synchronized void put(final Promise<Msg> p) throws InterruptedException {
    if (isOpen) {
      queue.put(p);
    } else {
      p.deliver(new IOException("Channel closed."));
    }
  }

  public synchronized Promise<Msg> take() {
    try {
      return queue.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
