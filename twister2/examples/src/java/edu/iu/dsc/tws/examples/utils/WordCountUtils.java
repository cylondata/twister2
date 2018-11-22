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
package edu.iu.dsc.tws.examples.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public final class WordCountUtils {
  private static final Logger LOG = Logger.getLogger(WordCountUtils.class.getName());

  private WordCountUtils() {
  }

  /**
   * Let assume we have 2 tasks per container and one additional for first container,
   * which will be the destination
   * @return task plan
   */
  public static TaskPlan createWordCountPlan(Config cfg,
                                             int workerID,
                                             List<JobMasterAPI.WorkerInfo> workerInfoList,
                                             int noOfTasks) {
    int noOfProcs = workerInfoList.size();
//    LOG.log(Level.INFO, "No of containers: " + noOfProcs);
    Map<Integer, Set<Integer>> executorToGraphNodes = new HashMap<>();
    Map<Integer, Set<Integer>> groupsToExeuctors = new HashMap<>();
    int thisExecutor = workerID;

    Map<String, List<JobMasterAPI.WorkerInfo>> containersPerNode = new HashMap<>();
    for (JobMasterAPI.WorkerInfo workerInfo : workerInfoList) {
      String name = Integer.toString(workerInfo.getWorkerID());
      List<JobMasterAPI.WorkerInfo> containerList;
      if (!containersPerNode.containsKey(name)) {
        containerList = new ArrayList<>();
        containersPerNode.put(name, containerList);
      } else {
        containerList = containersPerNode.get(name);
      }
      containerList.add(workerInfo);
    }

    int taskPerExecutor = noOfTasks / noOfProcs;
    for (int i = 0; i < noOfProcs; i++) {
      Set<Integer> nodesOfExecutor = new HashSet<>();
      for (int j = 0; j < taskPerExecutor; j++) {
        nodesOfExecutor.add(i * taskPerExecutor + j);
      }
      if (i == 0) {
        nodesOfExecutor.add(noOfTasks);
      }
      executorToGraphNodes.put(i, nodesOfExecutor);
    }

    int i = 0;
    // we take each container as an executor
    for (Map.Entry<String, List<JobMasterAPI.WorkerInfo>> e : containersPerNode.entrySet()) {
      Set<Integer> executorsOfGroup = new HashSet<>();
      for (JobMasterAPI.WorkerInfo workerInfo : e.getValue()) {
        executorsOfGroup.add(workerInfo.getWorkerID());
      }
      groupsToExeuctors.put(i, executorsOfGroup);
      i++;
    }

    LOG.fine(String.format("%d Executor To Graph: %s",
        workerID, executorToGraphNodes));
    LOG.fine(String.format("%d Groups to executors: %s",
        workerID, groupsToExeuctors));
    // now lets create the task plan of this, we assume we have map tasks in all the processes
    // and reduce task in 0th process
    return new TaskPlan(executorToGraphNodes, groupsToExeuctors, thisExecutor);
  }
}
