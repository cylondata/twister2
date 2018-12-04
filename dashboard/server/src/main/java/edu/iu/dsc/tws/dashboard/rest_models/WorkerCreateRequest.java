package edu.iu.dsc.tws.dashboard.rest_models;

import edu.iu.dsc.tws.dashboard.data_models.composite_ids.NodeId;

public class WorkerCreateRequest {

  private String jobId;
  private Long workerId;
  private String workerIP;
  private Integer workerPort;
  private NodeId node;
  private Integer computeResourceIndex;

  public Integer getWorkerPort() {
    return workerPort;
  }

  public void setWorkerPort(Integer workerPort) {
    this.workerPort = workerPort;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public Long getWorkerId() {
    return workerId;
  }

  public void setWorkerId(Long workerId) {
    this.workerId = workerId;
  }

  public String getWorkerIP() {
    return workerIP;
  }

  public void setWorkerIP(String workerIP) {
    this.workerIP = workerIP;
  }

  public NodeId getNode() {
    return node;
  }

  public void setNode(NodeId node) {
    this.node = node;
  }

  public Integer getComputeResourceIndex() {
    return computeResourceIndex;
  }

  public void setComputeResourceIndex(Integer computeResourceIndex) {
    this.computeResourceIndex = computeResourceIndex;
  }
}
