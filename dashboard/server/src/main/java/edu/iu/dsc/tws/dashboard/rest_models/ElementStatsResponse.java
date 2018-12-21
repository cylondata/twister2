package edu.iu.dsc.tws.dashboard.rest_models;

public class ElementStatsResponse {

  private long jobs;
  private long workers;
  private long nodes;
  private long clusters;

  public long getJobs() {
    return jobs;
  }

  public void setJobs(long jobs) {
    this.jobs = jobs;
  }

  public long getWorkers() {
    return workers;
  }

  public void setWorkers(long workers) {
    this.workers = workers;
  }

  public long getNodes() {
    return nodes;
  }

  public void setNodes(long nodes) {
    this.nodes = nodes;
  }

  public long getClusters() {
    return clusters;
  }

  public void setClusters(long clusters) {
    this.clusters = clusters;
  }
}
