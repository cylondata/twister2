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
package edu.iu.dsc.tws.master;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class WorkerWithState {
  private static final Logger LOG = Logger.getLogger(WorkerWithState.class.getName());

  private JobMasterAPI.WorkerState workerState;
  private JobMasterAPI.WorkerInfo workerInfo;
  private long pingTimestamp;

  public WorkerWithState(JobMasterAPI.WorkerInfo workerInfo) {
    this.workerInfo = workerInfo;
    workerState = JobMasterAPI.WorkerState.UNASSIGNED;
    pingTimestamp = -1;
  }

  public static InetAddress covertToIPAddress(String ipStr) {
    if (ipStr != null) {
      try {
        return InetAddress.getByName(ipStr);
      } catch (UnknownHostException e) {
        LOG.log(Level.SEVERE, "Can not covert the string to IP address. String: " + ipStr, e);
      }
    }
    return null;
  }

  public void setWorkerState(JobMasterAPI.WorkerState workerState) {
    this.workerState = workerState;
  }

  public JobMasterAPI.WorkerInfo getWorkerInfo() {
    return workerInfo;
  }

  public JobMasterAPI.WorkerState getWorkerState() {
    return workerState;
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
