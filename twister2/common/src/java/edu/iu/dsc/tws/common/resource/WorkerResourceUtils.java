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

import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;

public final class WorkerResourceUtils {
  public static final Logger LOG = Logger.getLogger(WorkerResourceUtils.class.getName());

  private WorkerResourceUtils() { }

  public static Map<String, List<WorkerComputeResource>> getWorkersPerNode(
      AllocatedResources allocatedResources,
      List<WorkerNetworkInfo> workerList) {

    List<WorkerComputeResource> computeResources = allocatedResources.getWorkerComputeResources();

    // get distinct NodeIPs
    List<String> distinctNodes = getDistinctNodeIPs(workerList);
    if (distinctNodes == null) {
      return null;
    }

    // build the list for each nodeIP
    Map<String, List<WorkerComputeResource>> workersPerNode = new HashMap<>();
    for (String nodeIP: distinctNodes) {
      List<Integer> workerIDList = getWorkerIDsForANode(nodeIP, workerList);
      ArrayList<WorkerComputeResource> resourceList = new ArrayList<>();
      for (Integer workerID: workerIDList) {
        WorkerComputeResource resource = computeResources.get(
            computeResources.indexOf(new WorkerComputeResource(workerID)));
        resourceList.add(resource);
      }

      workersPerNode.put(nodeIP, resourceList);
    }

    return workersPerNode;
  }

  public static List<String> getDistinctNodeIPs(List<WorkerNetworkInfo> workerList) {

    ArrayList<String> distinctNodes = new ArrayList<>();

    for (WorkerNetworkInfo workerNetworkInfo: workerList) {
      String nodeIP = workerNetworkInfo.getNodeInfo().getNodeIP();
      if (nodeIP == null) {
        LOG.severe("NodeIP is null");
        return null;
      } else if (!distinctNodes.contains(nodeIP)) {
        distinctNodes.add(nodeIP);
      }
    }

    return distinctNodes;
  }

  public static List<Integer> getWorkerIDsForANode(String nodeIP,
                                                   List<WorkerNetworkInfo> workerList) {

    ArrayList<Integer> workerIDs = new ArrayList<>();

    for (WorkerNetworkInfo workerNetworkInfo: workerList) {
      if (nodeIP.equals(workerNetworkInfo.getNodeInfo().getNodeIP())) {
        workerIDs.add(workerNetworkInfo.getWorkerID());
      }
    }

    return workerIDs;
  }

}
