package riemann.java.client.tests;

import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.net.*;
import java.io.*;

import io.riemann.riemann.client.RiemannClient;
import io.riemann.riemann.Proto.Attribute;
import io.riemann.riemann.Proto.Event;
import io.riemann.riemann.Proto.Msg;

public abstract class Server {
	public int port;
	public Thread thread;
  public ServerSocket serverSocket;
  public LinkedBlockingQueue<Msg> received = new LinkedBlockingQueue<Msg>();

	public InetSocketAddress start() throws IOException {
    this.serverSocket = new ServerSocket(0);
    this.port = serverSocket.getLocalPort();
    this.serverSocket.setReceiveBufferSize(100);
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
          DataOutputStream out = null;
          DataInputStream in = null;

          try {
            // Accept connection
            while((sock = serverSocket.accept()) != null) {
              // Set up streams
              out = new DataOutputStream(sock.getOutputStream());
              in = new DataInputStream(sock.getInputStream());

              // Over each message
              while (true) {
                // Read length
                final int len = in.readInt();
                
                // Read message
                final byte[] data = new byte[len];
                in.readFully(data);
                final Msg request = Msg.parseFrom(data);
                
                // Log request
                received.put(request);

                // Handle message
                final Msg response = handle(request);
                
                // Write response
                out.writeInt(response.getSerializedSize());
                response.writeTo(out);
              }
            }
          } catch (SocketException e) {
            // Socket closed.
          } catch (EOFException e) {
            // Socket closed.
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
