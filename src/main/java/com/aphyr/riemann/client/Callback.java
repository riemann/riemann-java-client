package com.aphyr.riemann.client;

public interface Callback<T> {

  public void call(T value, Exception exception);

}
