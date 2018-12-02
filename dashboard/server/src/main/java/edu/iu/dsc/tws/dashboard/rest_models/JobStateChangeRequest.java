package edu.iu.dsc.tws.dashboard.rest_models;

import edu.iu.dsc.tws.dashboard.data_models.JobState;

public class JobStateChangeRequest {

  private JobState jobState;

  public JobState getJobState() {
    return jobState;
  }

  public void setJobState(JobState jobState) {
    this.jobState = jobState;
  }
}
