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

import java.util.ArrayList;
import java.util.Objects;

/**
 * this class represents the physical machine properties a worker runs on
 */

public class NodeInfo {
  private String nodeIP;
  private String rackName;
  private String dataCenterName;

  public NodeInfo(String nodeIP, String rackName, String dataCenterName) {
    this.nodeIP = nodeIP;
    this.rackName = rackName;
    this.dataCenterName = dataCenterName;

    // make empty strings null, since protobuff returns empty string for non-set fields
    if ("".equals(nodeIP)) {
      this.nodeIP = null;
    }

    if ("".equals(rackName)) {
      this.rackName = null;
    }

    if ("".equals(dataCenterName)) {
      this.dataCenterName = null;
    }
  }

  public String getNodeIP() {
    return nodeIP;
  }

  public String getRackName() {
    return rackName;
  }

  public String getDataCenterName() {
    return dataCenterName;
  }

  public boolean hasNodeIP() {
    return nodeIP != null;
  }

  public boolean hasRackName() {
    return rackName != null;
  }

  public boolean hasDataCenterName() {
    return dataCenterName != null;
  }

  @Override
  public String toString() {
    return "NodeInfo{"
        + "nodeIP='" + nodeIP + '\''
        + ", rackName='" + rackName + '\''
        + ", dataCenterName='" + dataCenterName + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    if (nodeIP == null) {
      return false;
    }

    NodeInfo nodeInfo = (NodeInfo) o;
    return nodeIP.equals(nodeInfo.nodeIP);
  }

  @Override
  public int hashCode() {

    return Objects.hash(nodeIP);
  }

  /**
   * encode the given NodeInfo object fields as a single line String
   * each field is separated by a comma
   * @param nodeInfo
   * @return
   */
  public static String encodeNodeInfo(NodeInfo nodeInfo) {
    if (nodeInfo == null) {
      return "null,null,null";
    }
    return nodeInfo.nodeIP + "," + nodeInfo.rackName + "," + nodeInfo.dataCenterName;
  }

  /**
   * decode the given String that is encoded with encodeNodeInfo method
   * each field is separated by a comma
   * @return
   */
  public static NodeInfo decodeNodeInfo(String nodeInfoStr) {
    if (nodeInfoStr == null) {
      return null;
    }

    String[] fields = nodeInfoStr.split(",");
    if (fields.length != 3) {
      return null;
    }

    // null check
    String nodeIP = fields[0];
    if ("null".equalsIgnoreCase(nodeIP)) {
      nodeIP = null;
    }

    String rackName = fields[1];
    if ("null".equalsIgnoreCase(rackName)) {
      rackName = null;
    }

    String dcName = fields[2];
    if ("null".equalsIgnoreCase(dcName)) {
      dcName = null;
    }

    return new NodeInfo(nodeIP, rackName, dcName);
  }

  /**
   * encode the objects in the given list as a single line String
   * each NodeInfo object is separated by others with a semi colon
   * @return
   */
  public static String encodeNodeInfoList(ArrayList<NodeInfo> nodeInfoList) {
    String allNodesInfos = "";
    for (NodeInfo nodeInfo: nodeInfoList) {
      allNodesInfos += encodeNodeInfo(nodeInfo) + ";";
    }
    return allNodesInfos;
  }

  /**
   * decode the given String that is encoded with the method encodeNodeInfoList
   * each NodeInfo object is separated by others with a semi colon
   * @return
   */
  public static ArrayList<NodeInfo> decodeNodeInfoList(String nodeInfoListStr) {
    if (nodeInfoListStr == null) {
      return null;
    }

    String[] nodeInfoStrings = nodeInfoListStr.split(";");
    ArrayList<NodeInfo> nodeInfoList = new ArrayList<>();

    for (String nodeInfoStr: nodeInfoStrings) {
      NodeInfo nodeInfo = decodeNodeInfo(nodeInfoStr);
      if (nodeInfo != null) {
        nodeInfoList.add(nodeInfo);
      }
    }
    return nodeInfoList;
  }

  /**
   * convert the given list of NodeInfo objects to a multi-line string for printing
   * each NodeInfo object in a separate line
   * @return
   */
  public static String listToString(ArrayList<NodeInfo> nodeInfoList) {
    if (nodeInfoList == null) {
      return null;
    }

    String allNodesInfos = "";

    for (NodeInfo nodeInfo: nodeInfoList) {
      allNodesInfos += nodeInfo.toString() + "\n";
    }
    return allNodesInfos;
  }

}
