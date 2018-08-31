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

//
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
package edu.iu.dsc.tws.common.resource;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds information about the job compute resources for each worker.
 */
public class AllocatedResources {
  // the cluster name
  private String cluster;

  // id of this worker
  private int workerId;

  // list of resource workers
  private List<WorkerComputeResource> workers = new ArrayList<>();

  public AllocatedResources(String cluster, int workerId) {
    this.cluster = cluster;
    this.workerId = workerId;
  }

  public List<WorkerComputeResource> getWorkerComputeResources() {
    return workers;
  }

  public int getNumberOfWorkers() {
    return workers.size();
  }

  public void addWorkerComputeResource(WorkerComputeResource workerComputeResource) {
    this.workers.add(workerComputeResource);
  }

  public String getCluster() {
    return cluster;
  }

  public int getWorkerId() {
    return workerId;
  }

  //New method for taskscheduler
  public WorkerComputeResource getWorkerComputeResources(int id) {
    for (WorkerComputeResource w : workers) {
      if (w.getId() == id) {
        return w;
      }
    }
    return null;
  }
}
