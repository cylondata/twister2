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

import edu.iu.dsc.tws.proto.network.Network;

public class WorkerInfo {
  private static final Logger LOG = Logger.getLogger(WorkerInfo.class.getName());

  private Network.WorkerState workerState;
  private InetAddress ip = null;
  private int port;
  private int workerID;
  private long pingTimestamp;

  public WorkerInfo(int workerID) {
    this(workerID, (InetAddress) null, -1);
  }

  public WorkerInfo(int workerID, InetAddress ip, int port) {
    this.workerID = workerID;
    this.ip = ip;
    this.port = port;
    workerState = Network.WorkerState.UNASSIGNED;
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

  public void setWorkerState(Network.WorkerState workerState) {
    this.workerState = workerState;
  }

  public Network.WorkerState getWorkerState() {
    return workerState;
  }

  public InetAddress getIp() {
    return ip;
  }

  public int getPort() {
    return port;
  }

  public int getWorkerID() {
    return workerID;
  }

  public void setPingTimestamp(long pingTimestamp) {
    this.pingTimestamp = pingTimestamp;
  }

  public long getPingTimestamp() {
    return pingTimestamp;
  }

  public void setIp(InetAddress ip) {
    this.ip = ip;
  }

  public void setPort(int port) {
    this.port = port;
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
    return workerID == that.workerID;
  }

  @Override
  public int hashCode() {
    return Objects.hash(workerID);
  }
}
