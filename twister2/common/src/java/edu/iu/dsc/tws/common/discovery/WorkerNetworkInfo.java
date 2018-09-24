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

/**
 * Holds network address data for a Worker
 * It also has encoding and decoding of these data to save in ZooKeeper server nodes
 */

public class WorkerNetworkInfo {
  public static final Logger LOG = Logger.getLogger(WorkerNetworkInfo.class.getName());

  private int workerID;
  private InetAddress ip;
  private int port;
  private NodeInfo nodeInfo;

  public WorkerNetworkInfo(String ipStr, int port, int workerID) {
    this(convertStringToIP(ipStr), port, workerID, new NodeInfo(ipStr, null, null));
  }

  public WorkerNetworkInfo(InetAddress ip, int port, int workerID) {
    this(ip, port, workerID, new NodeInfo(ip.getHostAddress(), null, null));
  }

  public WorkerNetworkInfo(String ipStr, int port, int workerID, NodeInfo nodeInfo) {
    this(convertStringToIP(ipStr), port, workerID, nodeInfo);
  }

  public WorkerNetworkInfo(InetAddress ip, int port, int workerID, NodeInfo nodeInfo) {
    this.ip = ip;
    this.port = port;
    this.workerID = workerID;
    this.nodeInfo = nodeInfo;
  }

  public int getWorkerID() {
    return workerID;
  }

  public InetAddress getWorkerIP() {
    return ip;
  }

  public int getWorkerPort() {
    return port;
  }

  public NodeInfo getNodeInfo() {
    return nodeInfo;
  }

  /**
   * return ip:port as a string
   * @return worker name
   */
  public String getWorkerIpAndPort() {
    return ip.getHostAddress() + ":" + port;
  }

  public void setWorkerID(int workerID) {
    this.workerID = workerID;
  }

  private static InetAddress convertStringToIP(String ipStr) {
    try {
      return InetAddress.getByName(ipStr);
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Can not convert the given string to IP: " + ipStr, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof WorkerNetworkInfo) {
      WorkerNetworkInfo theOther = (WorkerNetworkInfo) o;
      if (this.workerID == theOther.workerID
          && this.ip.equals(theOther.ip)
          && this.port == theOther.port) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getWorkerIpAndPort(), workerID);
  }

  /**
   * encode the given WorkerNetworkInfo object fields as a single line of a String
   * encoding has two parts:
   *   first part consists of ip, port and workerID separated by comma
   *   second part consists of NodeInfo object
   * two parts are separated by a semicolon
   * @return
   */
  public static String encodeWorkerNetworkInfo(WorkerNetworkInfo networkInfo) {
    return networkInfo.ip.getHostAddress() + "," + networkInfo.port + "," + networkInfo.workerID
        + ";" + NodeInfo.encodeNodeInfo(networkInfo.nodeInfo);
  }

  /**
   * decode the given String that is encoded with the method encodeWorkerNetworkInfo
   * each field is separated by a comma
   * @return
   */
  public static WorkerNetworkInfo decodeWorkerNetworkInfo(String networkInfoStr) {
    if (networkInfoStr == null) {
      return null;
    }

    String[] twoParts = networkInfoStr.split(";");
    NodeInfo nodeInfo = NodeInfo.decodeNodeInfo(twoParts[1]);

    String[] fields = twoParts[0].split(",");
    if (fields.length != 3) {
      return null;
    }

    WorkerNetworkInfo workerNetworkInfo = new WorkerNetworkInfo(fields[0],
        Integer.parseInt(fields[1]),
        Integer.parseInt(fields[2]),
        nodeInfo);

    return workerNetworkInfo;
  }

  /**
   * convert the worker list to string for logging purposes
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
      buffer.append(String.format("%d workerID=%d IP:Port='%s' %s\n",
          i++, worker.getWorkerID(), worker.getWorkerIpAndPort(), worker.nodeInfo));
    }

    return buffer.toString();
  }

  @Override
  public String toString() {
    return "WorkerNetworkInfo{"
        + "workerID=" + workerID
        + ", ip=" + ip.getHostAddress()
        + ", port=" + port
        + ", nodeInfo=" + nodeInfo
        + '}';
  }
}
