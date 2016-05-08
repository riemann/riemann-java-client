package io.riemann.riemann.client;

/**
 * Exception-reporting callback.
 *
 */
public interface ExceptionReporter {
  public void reportException(final Throwable t);
}
