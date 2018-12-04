package edu.iu.dsc.tws.dashboard.data_models.composite_ids;

import java.io.Serializable;

public class WorkerId implements Serializable {

  private Long workerId;

  private String job;

  public Long getWorkerId() {
    return workerId;
  }

  public void setWorkerId(Long workerId) {
    this.workerId = workerId;
  }

  public String getJob() {
    return job;
  }

  public void setJob(String job) {
    this.job = job;
  }
}
