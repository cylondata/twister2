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
import java.util.logging.Logger;

import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.NodeInfo;

/**
 * Utility methods for NodeInfo objects
 */

public final class NodeInfoUtil {
  private static final Logger LOG = Logger.getLogger(NodeInfoUtil.class.getName());

  private NodeInfoUtil() { }

  /**
   * do not set if any one of the parameters are null
   * @param nodeIP
   * @param rackName
   * @param dataCenterName
   * @return
   */
  public static NodeInfo createNodeInfo(String nodeIP, String rackName, String dataCenterName) {

    NodeInfo.Builder builder = NodeInfo.newBuilder();

    if (nodeIP != null) {
      builder.setNodeIP(nodeIP);
    }

    if (rackName != null) {
      builder.setRackName(rackName);
    }

    if (dataCenterName != null) {
      builder.setDataCenterName(dataCenterName);
    }

    return builder.build();
  }

  /**
   * encode the given NodeInfo object fields as a single line String
   * each field is separated by a comma
   * if the given NodeInfo is null, then return "null,null,null"
   * @param nodeInfo
   * @return
   */
  public static String encodeNodeInfo(NodeInfo nodeInfo) {
    if (nodeInfo == null) {
      return "null,null,null";
    }

    String nodeIP = nodeInfo.getNodeIP();
    if (nodeInfo.getNodeIP().isEmpty()) {
      nodeIP = "null";
    }

    String rackName = nodeInfo.getRackName();
    if (nodeInfo.getRackName().isEmpty()) {
      rackName = "null";
    }

    String dcName = nodeInfo.getDataCenterName();
    if (nodeInfo.getDataCenterName().isEmpty()) {
      dcName = "null";
    }

    return nodeIP + "," + rackName + "," + dcName;
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

    return createNodeInfo(nodeIP, rackName, dcName);
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
   * each NodeInfoUtil object is separated by others with a semi colon
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

  /**
   * find the NodeInfo object with the given nodeIP and return it
   * @return
   */
  public static NodeInfo getNodeInfo(ArrayList<NodeInfo> nodeInfoList, String nodeIP) {
    if (nodeInfoList == null || nodeIP == null) {
      return null;
    }

    for (NodeInfo nodeInfo: nodeInfoList) {
      if (nodeIP.equals(nodeInfo.getNodeIP())) {
        return nodeInfo;
      }
    }
    return null;
  }

}
