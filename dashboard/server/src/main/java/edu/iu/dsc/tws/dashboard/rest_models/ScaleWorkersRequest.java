package edu.iu.dsc.tws.dashboard.rest_models;

public class ScaleWorkersRequest {

  private int change;
  private int numberOfWorkers;

  public int getChange() {
    return change;
  }

  public void setChange(int change) {
    this.change = change;
  }

  public int getNumberOfWorkers() {
    return numberOfWorkers;
  }

  public void setNumberOfWorkers(int numberOfWorkers) {
    this.numberOfWorkers = numberOfWorkers;
  }
}
