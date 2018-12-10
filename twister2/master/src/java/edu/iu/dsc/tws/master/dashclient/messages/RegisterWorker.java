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
package edu.iu.dsc.tws.master.dashclient.messages;

import edu.iu.dsc.tws.master.dashclient.models.Node;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * RegisterWorker message to send with json to Dashboard from JobMaster
 */

public class RegisterWorker {

  private String jobID;
  private int workerID;
  private String workerIP;
  private int workerPort;
  private int computeResourceIndex;
  private String state;
  private Node node;

  public RegisterWorker() { }

  public RegisterWorker(String jobID, JobMasterAPI.WorkerInfo workerInfo) {
    this.jobID = jobID;
    this.workerID = workerInfo.getWorkerID();
    this.workerIP = workerInfo.getWorkerIP();
    this.workerPort = workerInfo.getPort();
    this.computeResourceIndex = workerInfo.getComputeResource().getIndex();
    this.node = new Node(workerInfo.getNodeInfo());
    this.state = JobMasterAPI.WorkerState.STARTING.name();
  }

  // Getter Methods

  public int getComputeResourceIndex() {
    return computeResourceIndex;
  }

  public String getJobID() {
    return jobID;
  }

  public Node getNode() {
    return node;
  }

  public String getWorkerIP() {
    return workerIP;
  }

  public int getWorkerID() {
    return workerID;
  }

  public int getWorkerPort() {
    return workerPort;
  }

  public String getState() {
    return state;
  }

  // Setter Methods

  public void setComputeResourceIndex(int computeResourceIndex) {
    this.computeResourceIndex = computeResourceIndex;
  }

  public void setJobID(String jobID) {
    this.jobID = jobID;
  }

  public void setNode(Node nodeObject) {
    this.node = nodeObject;
  }

  public void setWorkerIP(String workerIP) {
    this.workerIP = workerIP;
  }

  public void setWorkerID(int workerID) {
    this.workerID = workerID;
  }

  public void setWorkerPort(int workerPort) {
    this.workerPort = workerPort;
  }

  public void setState(String state) {
    this.state = state;
  }

  @Override
  public String toString() {
    return "{"
        + "\"workerID\": " + workerID + ", "
        + "\"workerIP\": " + "\"" + workerIP + "\", "
        + "\"workerPort\": " + workerPort + ", "
        + "\"jobID\": " + "\"" + jobID + "\", "
        + "\"computeResourceIndex\": " + computeResourceIndex + ", "
        + node
        + '}';
  }
}
