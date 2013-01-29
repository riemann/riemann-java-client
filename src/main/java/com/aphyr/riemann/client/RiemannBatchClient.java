package com.aphyr.riemann.client;

// Wraps any AbstractRiemann client for use in high-throughput loads. Behaves
// exactly like the underlying client, except that calls to sendEvents and
// sendEventsWithAck (including client.event()...send()), are
// batched together into single messages for more efficient transfer.
//
// Can improve bulk throughput by at least an order of magnitude, without
// requiring coordination from writer threads--at the cost of significantly
// higher latencies.
//
// Note that Messages do not explain *which* events fail. Every event in a
// message may raise an exception because of a problem with a single event.
// There is no gaurantee as to *which* events were successfully received. Since
// Riemann considers almost every event valid, and will only return failures in
// the event of resource problems, this should be acceptable for most
// scenarios. 
//
// To maximize throughput, BatchingRiemannClient is purely reactive, lockfree,
// and makes no guarantees about event latency. Latency of sendEvents() may vary
// widely, depending on whether or not that call flushed the buffer. You may
// call flush() to force buffered events to be written. Flush is automatically
// called at disconnect(). Calls to flush() may not clear the full buffer if
// other threads are actively writing events.
//
// If you need finer-grained control over events, access the underlying client
// directly. Arbitrarily many BatchingRiemannClients may operate over a single
// underlying client.

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.LinkedTransferQueue;
import java.util.ArrayList;
import java.util.List;
import com.aphyr.riemann.Proto.Msg;
import com.aphyr.riemann.Proto.Event;
import java.io.IOException;
import java.net.UnknownHostException;

public class RiemannBatchClient extends AbstractRiemannClient {
  public final int batchSize;
  public final AtomicInteger bufferSize = new AtomicInteger();
  public final LinkedTransferQueue<Write> buffer = new
    LinkedTransferQueue<Write>();
  public final AbstractRiemannClient client;

  // Maximum time, in ms, we can wait for an event to be processed.
  public final AtomicLong readPromiseTimeout = new AtomicLong(5000);

  // Used when we don't care about results.
  public final Promise<Boolean> blackhole = new Promise<Boolean>();

  public RiemannBatchClient(final AbstractRiemannClient client) throws
    UnknownHostException {
    this(10, client);
  }

  public RiemannBatchClient(final int batchSize, final AbstractRiemannClient
      client) throws UnknownHostException {
    this.batchSize = batchSize;
    this.client = client;
  }

  // Fire and forget. No guarantees on delivery.
  public void sendEvents(final List<Event> events) {
    for (Event event : events) {
      queue(new Write(event, blackhole));
    }
  }

  // Blocks until all events have been received. Throws if any event was *not*
  // acknowledged.
  public Boolean sendEventsWithAck(final List<Event> events) throws
    IOException, ServerError, MsgTooLargeException {

    final ArrayList<Promise<Boolean>> promises = 
      new ArrayList<Promise<Boolean>>(events.size());

    // Queue events
    Write write;
    for (Event event : events) {
      write = new Write(event);
      promises.add(write.promise);
      queue(write);
    }

    // Await promises
    for (Promise<Boolean> promise : promises) {
      try {
        if (! promise.await(readPromiseTimeout.get(), 
              TimeUnit.MILLISECONDS, false)) {
          throw new IOException("Timed out waiting for response promise.");
        }
      } catch (RuntimeException e) {
        // Extract IOExceptions
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        } else if (e.getCause() instanceof ServerError) {
          throw (ServerError) e.getCause();
        } else if (e.getCause() instanceof MsgTooLargeException) {
          throw (MsgTooLargeException) e.getCause();
        }
        throw e;
      }
    }

    return true;
  }

  // Hey, I just called you
  // And this is crazy
  // But take this event
  // And flush queues maybe
  public void queue(final Write write) {
    buffer.put(write);
    if (batchSize <= bufferSize.addAndGet(1)) {
      flush();
    }
  }

  // Flushes up to batchSize writes from the queue, and fulfills their
  // promises. Should never throw; any exceptions go to the corresponding
  // promises.
  public int flush() {
    final int maxWrites = Math.min(batchSize, bufferSize.get());
    // Allocate space for writes
    final ArrayList<Write> writes = new ArrayList<Write>(maxWrites);
    // Suck down elements from queue
    buffer.drainTo(writes, maxWrites);
    // Update count
    bufferSize.addAndGet(-1 * writes.size());

    try {
      // Build message
      final Msg.Builder message = Msg.newBuilder();
      for (Write write : writes) {
        message.addEvents(write.event);
      }

      // Send message
      validate(
          sendRecvMessage(
            message.build()));

      // Fulfill promises
      for (Write write : writes) {
        write.promise.deliver(true);
      }
    } catch (RuntimeException e) {
      // Deliver runtime exceptions
      for (Write write : writes) {
        write.promise.deliver(e);
      }
    } catch (Throwable t) {
      // Wrap and deliver any other exceptions
      final RuntimeException ex = new RuntimeException(t);
      for (Write write : writes) {
        write.promise.deliver(ex);
      }
    }

    return writes.size();
  }

  public void sendMessage(final Msg message) throws IOException,
         MsgTooLargeException {
    client.sendMessage(message);
  }

  public Msg recvMessage() throws IOException {
    return client.recvMessage();
  }

  public Msg sendRecvMessage(final Msg message) throws IOException,
         MsgTooLargeException {
    return client.sendRecvMessage(message);
  }

  public Msg sendMaybeRecvMessage(final Msg message) throws
    IOException, MsgTooLargeException {
    return client.sendMaybeRecvMessage(message);
  }

  public boolean isConnected() {
    return client.isConnected();
  }

  public void connect() throws IOException {
    client.connect();
  }

  public void disconnect() throws IOException {
    try {
      flush();
    } finally {
      client.disconnect();
    }
  }

  // Combines an Event with a promise to fulfill when received.
  public class Write {
    public final Event event;
    public final Promise<Boolean> promise;

    public Write(final Event event, final Promise promise) {
      this.event = event;
      this.promise = promise;
    }

    public Write(final Event event) {
      this.event = event;
      this.promise = new Promise<Boolean>();
    }
  }
}
