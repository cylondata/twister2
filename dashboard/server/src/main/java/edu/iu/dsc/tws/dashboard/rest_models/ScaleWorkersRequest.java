package edu.iu.dsc.tws.dashboard.rest_models;

import java.util.ArrayList;
import java.util.List;

public class ScaleWorkersRequest {

  private int change;
  private int numberOfWorkers;
  private List<Long> killedWorkers = new ArrayList<>();

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

  public List<Long> getKilledWorkers() {
    return killedWorkers;
  }

  public void setKilledWorkers(List<Long> killedWorkers) {
    this.killedWorkers = killedWorkers;
  }
}
