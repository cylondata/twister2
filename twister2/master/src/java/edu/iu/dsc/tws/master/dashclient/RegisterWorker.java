//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.master.dashclient;

public class RegisterWorker {

//  @JsonProperty
  private int workerId;

//  @JsonProperty
  private String workerIP;

//  @JsonProperty
  private int workerPort;

//  @JsonProperty
  private String jobId;

//  @JsonProperty
  private int computeResourceIndex;

//  @JsonProperty
  private Node node;


  // Getter Methods

  public int getComputeResourceIndex() {
    return computeResourceIndex;
  }

  public String getJobId() {
    return jobId;
  }

  public Node getNode() {
    return node;
  }

  public String getWorkerIP() {
    return workerIP;
  }

  public int getWorkerId() {
    return workerId;
  }

  public int getWorkerPort() {
    return workerPort;
  }

  // Setter Methods

  public void setComputeResourceIndex(int computeResourceIndex) {
    this.computeResourceIndex = computeResourceIndex;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public void setNode(Node nodeObject) {
    this.node = nodeObject;
  }

  public void setWorkerIP(String workerIP) {
    this.workerIP = workerIP;
  }

  public void setWorkerId(int workerId) {
    this.workerId = workerId;
  }

  public void setWorkerPort(int workerPort) {
    this.workerPort = workerPort;
  }

  @Override
  public String toString() {
    return "{"
        + "\"workerId\": " + workerId + ", "
        + "\"workerIP\": " + "\"" + workerIP + "\", "
        + "\"workerPort\": " + workerPort + ", "
        + "\"jobId\": " + "\"" + jobId + "\", "
        + "\"computeResourceIndex\": " + computeResourceIndex + ", "
        + node
        + '}';
  }
}
