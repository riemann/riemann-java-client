package com.aphyr.riemann.client;

import com.aphyr.riemann.Proto.Msg;
import com.aphyr.riemann.Proto.Event;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import java.lang.Thread;
import java.lang.Runnable;
import java.io.IOException;
import java.net.UnknownHostException;

// An asynchronous, nonblocking, mostly lockfree wrapper around a TcpClient.  N
// threads may freely submit messages to this client; they are enqueued and
// processed by a pair of sender/receiver threads.
public class RiemannThreadedClient extends AbstractRiemannClient {
  public final RiemannTcpClient client;
  public final LinkedBlockingQueue<Write> writes;
  public final LinkedBlockingQueue<Write> inflight;
  public final AtomicBoolean writerRunning = new AtomicBoolean(false);
  public final AtomicBoolean readerRunning = new AtomicBoolean(false);
  public CountDownLatch writerLatch;
  public CountDownLatch readerLatch;
  
  public final long writeQueueTimeout = 10;
  public final long inflightQueueTimeout = 10;
  public final int writeCapacity = 100;
  public final int inflightCapacity = 100;
  public final long disconnectTimeout = 1000;
  public final long readPromiseTimeout = 5000;

  public RiemannThreadedClient(RiemannTcpClient client) throws UnknownHostException {
    this.client = client;
    this.inflight = new LinkedBlockingQueue<Write>(inflightCapacity);
    this.writes = new LinkedBlockingQueue<Write>(inflightCapacity);
  }

  // May block indefinitely if the write queue is full. Doesn't really throw.
  // Java is full of lies. 
  @Override
  public void sendMessage(Msg message) throws IOException {
    try {
      writes.put(new Write(message));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  // LMAO if you consider this a type system
  @Override
  public Msg recvMessage() throws IOException {
    throw new RuntimeException("Can't recvMessage() from a RiemannThreadedClient.");
  }

  // Returns a promise.
  public Promise<Msg> sendAsyncMessage(Msg message) {
    final Write write = new Write(message);
    try {
      writes.put(write);
      return write.promise;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  // May block indefinitely if the write queue is full. Waits for the resulting
  // message.
  @Override
  public Msg sendRecvMessage(Msg message) throws IOException {
    final Msg response = sendAsyncMessage(message).await(readPromiseTimeout,
        TimeUnit.MILLISECONDS);
    if (response == null) {
      throw new RuntimeException("Timed out waiting for response promise.");
    } else {
      return response;
    }
  }

  // Same as sendRecvMessage.
  @Override
  public Msg sendMaybeRecvMessage(Msg message) throws IOException {
    return sendRecvMessage(message);
  }

  @Override
  public boolean isConnected() {
    return client.isConnected();
  }

  @Override
  public synchronized void connect() throws IOException {
    client.connect();
    start();
  }

  @Override
  public synchronized void disconnect() throws IOException {
    try {
      stop();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      client.disconnect();
    }
  }

  // Pulls a Write off the writes queue, writes it to the socket, and
  // pushes a promise onto the inflight queue.
  public boolean write() {
    try {
      // Get an item from ze queue
      final Write write = writes.poll(writeQueueTimeout, TimeUnit.MILLISECONDS);
      if (write == null) {
        return false;
      } else {
        try {
//          System.out.println("Writing " + write.message.toString());
          client.sendMessage(write.message);
          // Inflight queue will block us until the reader has caught up
//          System.out.println("Written.");
          inflight.put(write);
//          System.out.println("Enqueued inflight");
          return true;
        } catch (RuntimeException e) {
          write.promise.deliver(e);
          return false;
        } catch (Throwable t) {
          write.promise.deliver(new RuntimeException(t));
          return false;
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
      return false;
    }
  }

  // Pulls a promise off the inflight queue, reads its value from the socket,
  // and fulfills the promise.
  public boolean read() {
    try {
      final Write write = inflight.poll(inflightQueueTimeout,
          TimeUnit.MILLISECONDS);
      if (write == null) {
        return false;
      } else {
        try {
//          System.out.println("Awaiting " + write.message.toString());
          write.promise.deliver(client.recvMessage());
//          System.out.println("Delivered.");
          return true;
        } catch (RuntimeException e) {
          write.promise.deliver(e);
          return false;
        } catch (Throwable t) {
          write.promise.deliver(new RuntimeException(t));
          return false;
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
      return false;
    }
  }

  // Start writing messages to the client.
  public synchronized RiemannThreadedClient startWriter() {
    if (writerRunning.get()) {
      return this;
    }

    writerLatch = new CountDownLatch(1);
    writerRunning.set(true);

    new Thread(new Runnable() {
      public void run() {
        while (writerRunning.get()) {
          write();
        }
        // Drain queue
//        System.out.println("Draining writer...");
        while (write()) { };
        writerLatch.countDown();
      }
    }).start();

    return this;
  }

  // Start reading responses from the client.
  public synchronized RiemannThreadedClient startReader() {
    if (readerRunning.get()) {
      return this;
    }

    readerLatch = new CountDownLatch(1);
    readerRunning.set(true);

    new Thread(new Runnable() {
      public void run() {
        Promise<Msg> promise;
        while (readerRunning.get()) {
          read();
        }
        // Drain queue
//        System.out.println("Draining reader...");
        while (read()) { };
        readerLatch.countDown();
      }
    }).start();

    return this;
  }

  // Gracefully stop the writer. Blocks.
  public synchronized RiemannThreadedClient stopWriter() throws
    InterruptedException {
//      System.out.println("Stop writer...");
    if (writerRunning.compareAndSet(true, false)) {
      writerLatch.await();
    }
//    System.out.println("Writer stopped.");
    return this;
  }

  // Gracefully stop the reader. Blocks.
  public synchronized RiemannThreadedClient stopReader() throws
    InterruptedException {
//    System.out.println("Stop reader...");
    if (readerRunning.compareAndSet(true, false)) {
      readerLatch.await();
    }
//    System.out.println("Reader stopped.");
    return this;
  }

  // Start both threads. Blocks.
  public synchronized RiemannThreadedClient start() {
//    System.out.println("Starting...");
    startReader();
    startWriter();
//    System.out.println("Started.");
    return this;
  }

  // Stop both threads. Blocks.
  public synchronized RiemannThreadedClient stop() throws InterruptedException {
//    System.out.println("Stopping...");
    stopWriter();
    stopReader();
//    System.out.println("Stopped.");
    return this;
  }

  // Combines an event with a promise to fulfill when received.
  public class Write {
    public final Msg message;
    public final Promise<Msg> promise = new Promise<Msg>();

    public Write(Msg message) {
      this.message = message;
    }
  }
}
