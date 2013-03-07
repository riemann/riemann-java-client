package riemann.java.client.tests;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.net.*;
import java.io.*;

import com.aphyr.riemann.client.RiemannClient;
import com.aphyr.riemann.Proto.Attribute;
import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.Proto.Msg;

public abstract class Server {
	public int port;
	public Thread thread;
  public ServerSocket serverSocket;

	public InetSocketAddress start() throws IOException {
		this.port = 9800 + new Random().nextInt(100);
    this.serverSocket = new ServerSocket(this.port);
		this.thread = mainThread(this.serverSocket);
		this.thread.start();

    return new InetSocketAddress(InetAddress.getLocalHost(), this.port);
	}

	public void stop() {
    if (this.thread != null) {
      this.thread.interrupt();
      this.thread = null;
    }
		this.port = -1;
		this.serverSocket = null;
	}

	public Thread mainThread(final ServerSocket serverSocket) {
    return new Thread() {
      @Override
      public void run() {
        try {
          Socket sock = null;
          OutputStream out = null;
          DataInputStream in = null;
          byte[] data;

          try {
            // Accept connection
            while((sock = serverSocket.accept()) != null) {
              // Set up streams
              out = sock.getOutputStream();
              in = new DataInputStream(sock.getInputStream());

              // Over each message
              while (true) {
                // Read length
                data = new byte[in.readInt()];
                // Read message
                in.readFully(data);
                // Parse, handle, and write response
                handle(Msg.parseFrom(data)).writeTo(out);
              }
            }
          } finally {
            if (out  != null) { out.close();  }
            if (in   != null) { in.close();   }
            if (sock != null) { sock.close(); }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
  }

  public abstract Msg handle(final Msg m);
}
