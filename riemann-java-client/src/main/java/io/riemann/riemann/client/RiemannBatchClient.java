package io.riemann.riemann.client;

// Wraps any Riemann client for use in high-throughput loads. Behaves
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
// There is no guarantee as to *which* events were successfully received. Since
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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.LinkedTransferQueue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import io.riemann.riemann.Proto.Msg;
import io.riemann.riemann.Proto.Event;
import java.io.IOException;

public class RiemannBatchClient implements IRiemannClient {
  public final int batchSize;
  public final AtomicInteger bufferSize = new AtomicInteger();
  public final LinkedTransferQueue<Write> buffer;
  public final IRiemannClient client;

  // Maximum time, in ms, we can wait for an event to be processed.
  public final AtomicLong readPromiseTimeout = new AtomicLong(5000);

  public RiemannBatchClient(final IRiemannClient client)
                           throws UnsupportedJVMException {
    this(client, 10);
  }

  public RiemannBatchClient(final IRiemannClient client,
                            final int batchSize)
                           throws UnsupportedJVMException {
    this.client = client;
    this.batchSize = batchSize;
    this.buffer = new java.util.concurrent.LinkedTransferQueue<Write>();
  }

  @Override
  public IPromise<Msg> sendMessage(final Msg message) {
    return client.sendMessage(message);
  }

  @Override
  public IPromise<Msg> sendEvents(final List<Event> events) {
    final ChainPromise<Msg> p = new ChainPromise<Msg>();

    // Queue up all events with this IPromise.
    for (Event event : events) {
      queue(new Write(event, p));
    }

    return p;
  }

  @Override
  public IPromise<Msg> sendEvents(final Event... events) {
    return sendEvents(Arrays.asList(events));
  }

  @Override
  public IPromise<Msg> sendEvent(final Event event) {
    final ChainPromise<Msg> p = new ChainPromise<Msg>();
    queue(new Write(event, p));
    return p;
  }

  @Override
  public IPromise<Msg> sendException(final String service, final Throwable t) {
    return RiemannClient.sendException(this, service, t);
  }

  @Override
  public IPromise<List<Event>> query(final String q) {
    return client.query(q);
  }

  @Override
  public EventDSL event() {
    return new EventDSL(this);
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
  public int flush2() {
    final int maxWrites = Math.min(batchSize, bufferSize.get());

    // Allocate space for writes
    final ArrayList<Write> writes = new ArrayList<Write>(maxWrites);

    // Suck down elements from queue
    buffer.drainTo(writes, maxWrites);

    // Update count
    bufferSize.addAndGet(-1 * writes.size());

    // Build message
    final Msg.Builder message = Msg.newBuilder();
    for (Write write : writes) {
      message.addEvents(write.event);
    }

    // Send message
    final IPromise<Msg> clientPromise = client.sendMessage(message.build());

    // And hook up all the response promises
    for (Write write : writes) {
      write.promise.attach(clientPromise);
    }

    try {
      client.flush();
    } catch (IOException e) {
      // not actually thrown by any implementation
    }
    return writes.size();
  }

  @Override
  public void flush() {
    flush2();
  }

  @Override
  public boolean isConnected() {
    return client.isConnected();
  }

  @Override
  public void connect() throws IOException {
    client.connect();
  }

  @Override
  public void close() {
    try {
      flush();
    } finally {
      client.close();
    }
  }

  @Override
  public void reconnect() throws IOException {
    client.reconnect();
  }

  // Returns the underlying client
  @Override
  public Transport transport() {
    return client;
  }

  // Combines an Event with a promise to fulfill when received.
  public class Write {
    public final Event event;
    public final ChainPromise<Msg> promise;

    public Write(final Event event, final ChainPromise promise) {
      this.event = event;
      this.promise = promise;
    }
  }
}
