package riemann.java.client.tests;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.atomic.AtomicReference;

import riemann.Proto.Msg;
import riemann.java.client.RiemannClient;
import riemann.java.client.RiemannUdpClient;

public class UdpClientTest extends AbstractClientTest {

	@Override
	Thread createServer(final int port, final AtomicReference<Msg> received) {
		return new Thread() {
			@Override
			public void run() {
				try {
					final DatagramSocket socket = new DatagramSocket(port);
					final byte[] buffer = new byte[1000];
					final DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
					serverStarted();
					socket.receive(packet);
					final byte[] data = new byte[packet.getLength()];
					System.arraycopy(packet.getData(), 0, data, 0, data.length);
					received.set(Msg.parseFrom(data));
					serverRecevied();
				} catch (Exception e) {
					throw new RuntimeException(e.getMessage(), e);
				}
			};
		};
	}

	@Override
	RiemannClient createClient(int port) throws IOException {
		return new RiemannUdpClient(port);
	}
}
