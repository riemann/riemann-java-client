package riemann.java.client.tests;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicReference;

import riemann.Proto.Msg;
import riemann.java.client.RiemannClient;
import riemann.java.client.RiemannTcpClient;

public class TcpClientTest extends AbstractClientTest {

	@Override
	Thread createServer(final int port, final AtomicReference<Msg> received) {
		return new Thread() {
			@Override
			public void run() {
				try {
					final ServerSocket server = new ServerSocket(port);
					serverStarted();
					final Socket socket = server.accept();
					final InputStream input = socket.getInputStream();
					final byte[] data = new byte[input.available()];
					input.read(data);
					received.set(Msg.parseFrom(data));
					serverRecevied();
				} catch (Exception e) {
					e.printStackTrace();
				}
			};
		};
	}

	@Override
	RiemannClient createClient(int port) throws UnknownHostException {
		return new RiemannTcpClient(new InetSocketAddress(InetAddress.getLocalHost(), port));
	}
}
