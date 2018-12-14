package edu.iu.dsc.tws.dashboard.rest_models;

import edu.iu.dsc.tws.dashboard.data_models.composite_ids.NodeId;

import java.util.HashMap;

public class WorkerCreateRequest {

  private String jobID;
  private Long workerID;
  private String workerIP;
  private Integer workerPort;
  private NodeId node;
  private Integer computeResourceIndex;
  private HashMap<String, Integer> additionalPorts = new HashMap<>();

  public HashMap<String, Integer> getAdditionalPorts() {
    return additionalPorts;
  }

  public void setAdditionalPorts(HashMap<String, Integer> additionalPorts) {
    this.additionalPorts = additionalPorts;
  }

  public Integer getWorkerPort() {
    return workerPort;
  }

  public void setWorkerPort(Integer workerPort) {
    this.workerPort = workerPort;
  }

  public String getJobID() {
    return jobID;
  }

  public void setJobID(String jobID) {
    this.jobID = jobID;
  }

  public Long getWorkerID() {
    return workerID;
  }

  public void setWorkerID(Long workerID) {
    this.workerID = workerID;
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
