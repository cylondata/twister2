package edu.iu.dsc.tws.dashboard.data_models.composite_ids;

import java.io.Serializable;
import java.util.Objects;

public class WorkerId implements Serializable {

  private Long workerID;

  private String job;

  public Long getWorkerID() {
    return workerID;
  }

  public void setWorkerID(Long workerID) {
    this.workerID = workerID;
  }

  public String getJob() {
    return job;
  }

  public void setJob(String job) {
    this.job = job;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WorkerId workerId = (WorkerId) o;
    return Objects.equals(workerID, workerId.workerID) &&
            Objects.equals(job, workerId.job);
  }

  @Override
  public int hashCode() {

    return Objects.hash(workerID, job);
  }
}
