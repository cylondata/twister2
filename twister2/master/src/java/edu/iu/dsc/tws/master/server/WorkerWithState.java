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

import java.util.Objects;
import java.util.logging.Logger;

import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerInfo;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerState;

/**
 * JobMaster keeps WorkerInfos with state
 * we keep all state history in an ArrayList
 */

public class WorkerWithState {
  private static final Logger LOG = Logger.getLogger(WorkerWithState.class.getName());

  private WorkerInfo workerInfo;
  private WorkerState state;

  public WorkerWithState(WorkerInfo workerInfo, WorkerState state) {
    this.workerInfo = workerInfo;
    this.state = state;
  }

  public void updateState(WorkerState workerState) {
    state = workerState;
  }

  public WorkerInfo getWorkerInfo() {
    return workerInfo;
  }

  public WorkerState getState() {
    return state;
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
    return !workerInfo.getNodeInfo().getNodeIP().isEmpty();
  }

  public boolean hasRackName() {
    return !workerInfo.getNodeInfo().getRackName().isEmpty();
  }

  public boolean hasDataCenterName() {
    return !workerInfo.getNodeInfo().getDataCenterName().isEmpty();
  }

  public void setWorkerInfo(WorkerInfo workerInfo) {
    this.workerInfo = workerInfo;
  }

  /**
   * return true if the worker status is either or STARTED, RESTARTED
   * @return
   */
  public boolean running() {
    return state == WorkerState.STARTED
        || state == WorkerState.RESTARTED;
  }


  /**
   * return true if the worker status is either or STARTED, RESTARTED, COMPLETED
   * @return
   */
  public boolean startedOrCompleted() {
    return state == WorkerState.STARTED
        || state == WorkerState.RESTARTED
        || state == WorkerState.COMPLETED;
  }

  /**
   * if the worker state is COMPLETED
   * @return
   */
  public boolean completed() {
    return state == WorkerState.COMPLETED;
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
