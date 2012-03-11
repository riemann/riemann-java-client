package riemann.java.client;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import riemann.Proto.Msg;

public class RiemannTcpClient extends RiemannClient {

	private Socket socket;
	private OutputStream sender;

	public RiemannTcpClient() throws UnknownHostException {}

	public RiemannTcpClient(int port) throws UnknownHostException {
		super(port);
	}

	public RiemannTcpClient(InetSocketAddress server) {
		super(server);
	}

	@Override
	protected void doSend(Msg message) throws IOException {
		message.writeTo(this.sender);
	}

	@Override
	public boolean isConnected() {
		return this.socket != null && this.socket.isConnected();
	}

	@Override
	public void connect() throws IOException {
		this.socket = new Socket(super.server.getAddress(), super.server.getPort());
		this.sender = this.socket.getOutputStream();
	}

	@Override
	public void disconnect() throws IOException {
		this.socket.close();
	}
}