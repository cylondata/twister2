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

import edu.iu.dsc.tws.master.dashclient.models.ComputeResource;
import edu.iu.dsc.tws.master.dashclient.models.JobState;
import edu.iu.dsc.tws.master.dashclient.models.Node;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * RegisterJob message to send with json to Dashboard from JobMaster
 */

public class RegisterJob {
  private String jobID;
  private String jobName;
  private int numberOfWorkers;
  private String state;
  private String workerClass;
  private Node node;
  private ComputeResource[] computeResources;

  public RegisterJob() {
  }

  public RegisterJob(String jobID, JobAPI.Job job, JobMasterAPI.NodeInfo nodeInfo) {
    this.jobID = jobID;
    this.jobName = job.getJobName();
    this.numberOfWorkers = job.getNumberOfWorkers();
    this.state = JobState.STARTING.name();
    this.workerClass = job.getWorkerClassName();
    this.node = new Node(nodeInfo);

    this.computeResources = new ComputeResource[job.getComputeResourceList().size()];
    int i = 0;
    for (JobAPI.ComputeResource cr : job.getComputeResourceList()) {
      computeResources[i++] = new ComputeResource(cr);
    }
  }

  public String getJobID() {
    return jobID;
  }

  public String getJobName() {
    return jobName;
  }

  public int getNumberOfWorkers() {
    return numberOfWorkers;
  }

  public String getState() {
    return state;
  }

  public String getWorkerClass() {
    return workerClass;
  }

  public Node getNode() {
    return node;
  }

  public ComputeResource[] getComputeResources() {
    return computeResources;
  }

  public void setJobID(String jobID) {
    this.jobID = jobID;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public void setNumberOfWorkers(int numberOfWorkers) {
    this.numberOfWorkers = numberOfWorkers;
  }

  public void setState(String state) {
    this.state = state;
  }

  public void setWorkerClass(String workerClass) {
    this.workerClass = workerClass;
  }

  public void setNode(Node node) {
    this.node = node;
  }

  public void setComputeResources(ComputeResource[] computeResources) {
    this.computeResources = computeResources;
  }
}
