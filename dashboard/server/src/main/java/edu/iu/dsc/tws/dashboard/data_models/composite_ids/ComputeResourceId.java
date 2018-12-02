package edu.iu.dsc.tws.dashboard.data_models.composite_ids;

import java.io.Serializable;

public class ComputeResourceId implements Serializable {

  private Integer index;

  private String job;

  public Integer getIndex() {
    return index;
  }

  public void setIndex(Integer index) {
    this.index = index;
  }

  public String getJob() {
    return job;
  }

  public void setJob(String job) {
    this.job = job;
  }
}
