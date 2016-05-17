package io.riemann.riemann.client;

public class ServerError extends RuntimeException {
  public final String message;

  public ServerError(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
