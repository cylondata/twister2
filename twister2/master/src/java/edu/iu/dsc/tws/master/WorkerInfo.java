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

import edu.iu.dsc.tws.common.discovery.NodeInfo;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class WorkerInfo {
  private static final Logger LOG = Logger.getLogger(WorkerInfo.class.getName());

  private JobMasterAPI.WorkerState workerState;
  private WorkerNetworkInfo networkInfo;
  private long pingTimestamp;

  public WorkerInfo(int workerID, InetAddress ip, int port) {
    this(workerID, ip, port, null, null, null);
  }

  public WorkerInfo(int workerID, InetAddress ip, int port,
                    String nodeIP, String rackName, String dcname) {

    networkInfo = new WorkerNetworkInfo(ip, port, workerID, new NodeInfo(nodeIP, rackName, dcname));
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

  public JobMasterAPI.WorkerState getWorkerState() {
    return workerState;
  }

  public InetAddress getIp() {
    return networkInfo.getWorkerIP();
  }

  public int getPort() {
    return networkInfo.getWorkerPort();
  }

  public int getWorkerID() {
    return networkInfo.getWorkerID();
  }

  public String getNodeIP() {
    return networkInfo.getNodeInfo().getNodeIP();
  }

  public String getRackName() {
    return networkInfo.getNodeInfo().getRackName();
  }

  public String getDataCenterName() {
    return networkInfo.getNodeInfo().getDataCenterName();
  }

  public boolean hasNodeIP() {
    return networkInfo.getNodeInfo().hasNodeIP();
  }

  public boolean hasRackName() {
    return networkInfo.getNodeInfo().hasRackName();
  }

  public boolean hasDataCenterName() {
    return networkInfo.getNodeInfo().hasDataCenterName();
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

    WorkerInfo that = (WorkerInfo) o;
    return networkInfo.getWorkerID() == that.networkInfo.getWorkerID();
  }

  @Override
  public int hashCode() {
    return Objects.hash(networkInfo.getWorkerID());
  }
}
