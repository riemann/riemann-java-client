package riemann.java.client.tests;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicReference;

import com.aphyr.riemann.Proto.Msg;
import com.aphyr.riemann.client.RiemannClient;
import com.aphyr.riemann.client.RiemannTcpClient;

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
					received.set(recieve(socket));
					serverRecevied();
					send(socket);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			private void send(Socket socket) throws IOException {
				final OutputStream out = socket.getOutputStream();
				Msg.newBuilder().build().writeTo(out);
				out.close();
			}

			private Msg recieve(final Socket socket) throws IOException {
				final DataInputStream input = new DataInputStream(socket.getInputStream());
				final byte[] data = new byte[input.readInt()];
				input.readFully(data);
				return Msg.parseFrom(data);
			};
		};
	}

	@Override
	RiemannClient createClient(int port) throws UnknownHostException {
		return new RiemannTcpClient(new InetSocketAddress(InetAddress.getLocalHost(), port));
	}
}
