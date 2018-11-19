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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public final class WorkerResourceUtils {
  public static final Logger LOG = Logger.getLogger(WorkerResourceUtils.class.getName());

  private WorkerResourceUtils() { }

  public static Map<String, List<JobMasterAPI.WorkerInfo>> getWorkersPerNode(
      List<JobMasterAPI.WorkerInfo> workerList) {

    // get distinct NodeIPs
    List<String> distinctNodes = getDistinctNodeIPs(workerList);
    if (distinctNodes == null) {
      return null;
    }

    // build the list for each nodeIP
    Map<String, List<JobMasterAPI.WorkerInfo>> workersPerNode = new HashMap<>();
    for (String nodeIP: distinctNodes) {
      List<JobMasterAPI.WorkerInfo> workersOnANode = getWorkersOnANode(nodeIP, workerList);
      workersPerNode.put(nodeIP, workersOnANode);
    }

    return workersPerNode;
  }

  public static List<String> getDistinctNodeIPs(List<JobMasterAPI.WorkerInfo> workerList) {

    ArrayList<String> distinctNodes = new ArrayList<>();

    for (JobMasterAPI.WorkerInfo workerInfo: workerList) {
      String nodeIP = workerInfo.getNodeInfo().getNodeIP();
      if (nodeIP.isEmpty()) {
        LOG.severe("NodeIP is not set.");
        return null;
      } else if (!distinctNodes.contains(nodeIP)) {
        distinctNodes.add(nodeIP);
      }
    }

    return distinctNodes;
  }

  /**
   * get the list of WorkerInfo objects on the given node
   * @param nodeIP
   * @param workerList
   * @return
   */
  public static List<JobMasterAPI.WorkerInfo> getWorkersOnANode(String nodeIP,
                                                         List<JobMasterAPI.WorkerInfo> workerList) {

    ArrayList<JobMasterAPI.WorkerInfo> workerIDs = new ArrayList<>();

    for (JobMasterAPI.WorkerInfo workerInfo: workerList) {
      if (nodeIP.equals(workerInfo.getNodeInfo().getNodeIP())) {
        workerIDs.add(workerInfo);
      }
    }

    return workerIDs;
  }

}
