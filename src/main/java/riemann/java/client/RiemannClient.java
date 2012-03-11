package riemann.java.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import riemann.Proto.Event;
import riemann.Proto.Msg;
import riemann.Proto.State;

public abstract class RiemannClient {

	public static final int DEFAULT_PORT = 5555;

	protected final InetSocketAddress server;

	public RiemannClient(final InetSocketAddress server) {
		this.server = server;
	}

	public RiemannClient(final int port) throws UnknownHostException {
		this.server = new InetSocketAddress(InetAddress.getLocalHost(), port);
	}

	public RiemannClient() throws UnknownHostException {
		this(new InetSocketAddress(InetAddress.getLocalHost(), DEFAULT_PORT));
	}

	public void send(final State... states) throws IOException {
		doSend(Msg.newBuilder()
				.setOk(true)
				.addAllStates(Arrays.asList(states))
				.build());
	}

	public void send(final Event... events) throws IOException {
		doSend(Msg.newBuilder()
				.setOk(true)
				.addAllEvents(Arrays.asList(events))
				.build());
	}

	protected abstract void doSend(Msg message) throws IOException;

	public abstract boolean isConnected();

	public abstract void connect() throws IOException;

	public abstract void disconnect() throws IOException;
}