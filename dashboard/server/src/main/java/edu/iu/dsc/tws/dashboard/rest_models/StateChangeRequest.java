package edu.iu.dsc.tws.dashboard.rest_models;

public class StateChangeRequest<T> {

  private T state;

  public T getState() {
    return state;
  }

  public void setState(T state) {
    this.state = state;
  }
}
