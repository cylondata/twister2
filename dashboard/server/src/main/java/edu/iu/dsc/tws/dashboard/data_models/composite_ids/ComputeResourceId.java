package edu.iu.dsc.tws.dashboard.data_models.composite_ids;

import java.io.Serializable;
import java.util.Objects;

public class ComputeResourceId implements Serializable {

  private static final long serialVersionUID = 1L;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ComputeResourceId that = (ComputeResourceId) o;
    return Objects.equals(index, that.index)
        && Objects.equals(job, that.job);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, job);
  }
}
