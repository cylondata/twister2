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
package edu.iu.dsc.tws.common.discovery;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WorkerNetworkInfo {
  public static final Logger LOG = Logger.getLogger(WorkerNetworkInfo.class.getName());

  private InetAddress ip;
  private int port;
  private int workerID;

  public WorkerNetworkInfo(InetAddress ip, int port, int workerID) {
    this.ip = ip;
    this.port = port;
    this.workerID = workerID;
  }

  /**
   * workerName has both IP and port in the form of ip:port
   * @param workerName name of
   * @param workerID
   */
  public WorkerNetworkInfo(String workerName, int workerID) {
    this.ip = constructWorkerIP(workerName);
    this.port = Integer.parseInt(workerName.substring(workerName.indexOf(":") + 1));
    this.workerID = workerID;
  }

  /**
   * return ip:port as a string
   * @return worker name
   */
  public String getWorkerName() {
    return ip.getHostAddress() + ":" + port;
  }

  public int getWorkerID() {
    return workerID;
  }

  public InetAddress getWorkerIP() {
    return ip;
  }

  private InetAddress constructWorkerIP(String workerName) {

    String ipStr = workerName.substring(0, workerName.indexOf(":"));
    try {
      return InetAddress.getByName(ipStr);
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Can not convert the given address to IP: " + workerName, e);
      throw new RuntimeException(e);
    }
  }

  public int getWorkerPort() {
    return port;
  }

  public byte[] getWorkerIDAsBytes() {
    return Integer.toString(workerID).getBytes();
  }

  public static int getWorkerIDFromBytes(byte[] data) {
    return Integer.parseInt(new String(data));
  }

  /**
   * this is the inverse of getWorkerInfoAsString method
   * @return WorkerNetworkInfo
   */
  public static WorkerNetworkInfo getWorkerInfoFromString(String str) {
    if (str == null || str.length() < 4) {
      return null;
    }

    String workerName = str.substring(0, str.indexOf("="));
    String idStr = str.substring(str.indexOf("=") + 1, str.indexOf(";"));
    return new WorkerNetworkInfo(workerName, Integer.parseInt(idStr));
  }

  public String getWorkerInfoAsString() {
    return getWorkerName() + "=" + workerID + ";";
  }

  /**
   * parse job znode content and set the id of this worker
   * @param str from string
   */
  public static int getWorkerIDByParsing(String str, String workerName) {
    int workerNameIndex = str.indexOf(workerName);
    int idStartIndex = str.indexOf("=", workerNameIndex) + 1;
    int idEndIndex = str.indexOf(";", idStartIndex);
    String idStr = str.substring(idStartIndex, idEndIndex);
    return Integer.parseInt(idStr);
  }

  public void setWorkerID(int workerID) {
    this.workerID = workerID;
  }

  @Override
  public String toString() {
    return "workerName: " + getWorkerName() + " workerID: " + workerID;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof WorkerNetworkInfo) {
      WorkerNetworkInfo theOther = (WorkerNetworkInfo) o;
      if (this.workerID == theOther.workerID
          && this.getWorkerName().equals(theOther.getWorkerName())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getWorkerName(), workerID);
  }

  /**
   * convert the worker list to string for logging
   * @param workers
   * @return
   */
  public static String workerListAsString(List<WorkerNetworkInfo> workers) {
    if (workers == null) {
      return null;
    }

    StringBuilder buffer = new StringBuilder();
    buffer.append("Number of workers: ").append(workers.size()).append("\n");
    int i = 0;
    for (WorkerNetworkInfo worker : workers) {
      buffer.append(String.format("%d: workerID[%d] %s\n",
          i++, worker.getWorkerID(), worker.getWorkerName()));
    }

    return buffer.toString();
  }

}


