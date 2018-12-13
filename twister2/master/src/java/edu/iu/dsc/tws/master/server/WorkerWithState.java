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
package edu.iu.dsc.tws.master.server;

import java.util.ArrayList;
import java.util.Objects;
import java.util.logging.Logger;

import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * JobMaster keeps WorkerInfos with state
 * we keep all state history in an ArrayList
 */

public class WorkerWithState {
  private static final Logger LOG = Logger.getLogger(WorkerWithState.class.getName());

  private JobMasterAPI.WorkerInfo workerInfo;
  private ArrayList<JobMasterAPI.WorkerState> states;
  private long pingTimestamp;

  public WorkerWithState(JobMasterAPI.WorkerInfo workerInfo) {
    this.workerInfo = workerInfo;
    states = new ArrayList<>();
    pingTimestamp = -1;
  }

  public void addWorkerState(JobMasterAPI.WorkerState workerState) {
    states.add(workerState);
  }

  public JobMasterAPI.WorkerInfo getWorkerInfo() {
    return workerInfo;
  }

  public JobMasterAPI.WorkerState getLastWorkerState() {
    return states.get(states.size() - 1);
  }

  public String getIp() {
    return workerInfo.getWorkerIP();
  }

  public int getPort() {
    return workerInfo.getPort();
  }

  public int getWorkerID() {
    return workerInfo.getWorkerID();
  }

  public String getNodeIP() {
    return workerInfo.getNodeInfo().getNodeIP();
  }

  public String getRackName() {
    return workerInfo.getNodeInfo().getRackName();
  }

  public String getDataCenterName() {
    return workerInfo.getNodeInfo().getDataCenterName();
  }

  public boolean hasNodeIP() {
    return workerInfo.getNodeInfo().getNodeIP() == null ? false : true;
  }

  public boolean hasRackName() {
    return workerInfo.getNodeInfo().getRackName() == null ? false : true;
  }

  public boolean hasDataCenterName() {
    return workerInfo.getNodeInfo().getDataCenterName() == null ? false : true;
  }

  public void setPingTimestamp(long pingTimestamp) {
    this.pingTimestamp = pingTimestamp;
  }

  public long getPingTimestamp() {
    return pingTimestamp;
  }

  /**
   * if the worker became RUNNING in the past, return true.
   * Its current status can be RUNNING, COMPLETED or something else
   * @return
   */
  public boolean hasWorkerBecomeRunning() {
    return states.contains(JobMasterAPI.WorkerState.RUNNING) ? true : false;
  }

  /**
   * if the worker has COMPLETED in its state history
   * @return
   */
  public boolean hasWorkerCompleted() {
    return states.contains(JobMasterAPI.WorkerState.COMPLETED) ? true : false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    WorkerWithState that = (WorkerWithState) o;
    return workerInfo.getWorkerID() == that.workerInfo.getWorkerID();
  }

  @Override
  public int hashCode() {
    return Objects.hash(workerInfo.getWorkerID());
  }

  @Override
  public String toString() {
    return workerInfo.toString();
  }
}
