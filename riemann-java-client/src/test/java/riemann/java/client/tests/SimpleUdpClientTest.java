package riemann.java.client.tests;

import io.riemann.riemann.Proto;
import io.riemann.riemann.client.RiemannClient;
import io.riemann.riemann.client.ServerError;
import io.riemann.riemann.client.SimpleUdpTransport;
import org.junit.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SimpleUdpClientTest {
  @Test
  public void sendEventsTest() throws IOException, InterruptedException, ServerError {
    final DatagramSocket serverSocket = new DatagramSocket();
    try {
      final RiemannClient client = new RiemannClient(new SimpleUdpTransport(serverSocket.getLocalPort()));
      try {
        client.connect();
        sendTestMessages(serverSocket, client);
      } finally {
        client.close();
      }
    } finally {
      serverSocket.close();
    }
  }

  @Test
  public void sendEventsOverReconnectionTest() throws IOException, InterruptedException, ServerError {
    DatagramSocket serverSocket = new DatagramSocket();
    final int port = serverSocket.getLocalPort();
    try {
      final RiemannClient client = new RiemannClient(new SimpleUdpTransport(serverSocket.getLocalPort()));
      try {
        client.connect();
        assertTrue(client.isConnected());
        sendTestMessages(serverSocket, client);

        // Close listening socket
        serverSocket.close();

        // Expect send to drop messages silently
        final Proto.Event e = Util.createEvent();
        client.sendEvent(e);

        // Reopen listening socket
        serverSocket = new DatagramSocket(new InetSocketAddress(port));

        // Expect sent messages to be received again
        sendTestMessages(serverSocket, client);

      } finally {
        client.close();
        assertFalse(client.isConnected());
      }
    } finally {
      serverSocket.close();
    }
  }

  private void sendTestMessages(final DatagramSocket serverSocket, final RiemannClient client) throws IOException {
    for (int i = 0; i < 10; i++) {
      final byte[] buffer = new byte[16384];
      final DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
      final Proto.Event e = Util.createEvent();
      client.sendEvent(e);
      serverSocket.receive(packet);
      final Proto.Msg msg = Proto.Msg.getDefaultInstance().newBuilderForType().mergeFrom(packet.getData(), 0, packet.getLength()).build();
      assertEquals(e, Util.soleEvent(msg));
    }
  }

}
