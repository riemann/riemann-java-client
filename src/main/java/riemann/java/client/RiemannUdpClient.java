package riemann.java.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import riemann.Proto.Msg;

public final class RiemannUdpClient extends RiemannClient {

	private DatagramSocket client;

	public RiemannUdpClient() throws UnknownHostException {}

	public RiemannUdpClient(int port) throws IOException {
		super(port);
		this.client = new DatagramSocket();
	}

	public RiemannUdpClient(InetSocketAddress server) throws IOException {
		super(server);
		this.client = new DatagramSocket();
	}

	@Override
	protected void doSend(final Msg message) throws IOException {
		final ByteArrayOutputStream out = new ByteArrayOutputStream();
		message.writeTo(out);
		final byte[] payload = out.toByteArray();
		this.client.send(new DatagramPacket(payload, payload.length, this.server));
	}

	@Override
	public boolean isConnected() {
		return true;
	}

	@Override
	public void connect() {}

	@Override
	public void disconnect() {
		this.client.close();
	}
}
