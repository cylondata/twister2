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
package edu.iu.dsc.tws.task.cdfw;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * This schedule is the base method for making decisions to run the part of the task graph which
 * will be improved further with the complex logic. Now, based on the relations(parent -> child)
 * it will initiate the execution.
 */
public class CDFWScheduler implements ICDFWScheduler {
  private static final Logger LOG = Logger.getLogger(CDFWScheduler.class.getName());

  private List<JobMasterAPI.WorkerInfo> workerInfoList;
  private Set<Integer> workerList = new LinkedHashSet<>();

  //To store the scheduled dataflow task graph and their corresponding worker list
  private Map<DataFlowGraph, Set<Integer>> scheduledGraphMap = new LinkedHashMap<>();

  protected CDFWScheduler(List<JobMasterAPI.WorkerInfo> workerInfoList) {
    this.workerInfoList = workerInfoList;
  }

  @Override
  public Set<Integer> schedule(DataFlowGraph graphJob) {
    Set<Integer> scheduledGraph = scheduleGraphs(graphJob);
    return scheduledGraph;
  }

  /**
   * This method is able to schedule multiple dataflow graphs. It will return the map which
   * corresponds to the dataflow graph and their worker ids.
   */
  @Override
  public Map<DataFlowGraph, Set<Integer>> schedule(DataFlowGraph... dataFlowGraphs) {
    if (dataFlowGraphs.length == 1) {
      workerList = scheduleGraphs(dataFlowGraphs[0]);
      scheduledGraphMap.put(dataFlowGraphs[0], workerList);
    } else if (dataFlowGraphs.length > 1) {
      for (DataFlowGraph graph : dataFlowGraphs) {
        workerList = scheduleGraphs(graph);
        scheduledGraphMap.put(graph, workerList);
      }
    }
    return scheduledGraphMap;
  }

  /**
   * This method allocate the workers to the individual dataflow graphs which is
   * based on the requested workers and the available workers in the worker info list.
   */
  private Set<Integer> scheduleGraphs(DataFlowGraph graph) {
    if (workerInfoList.size() == graph.getWorkers()) {
      for (JobMasterAPI.WorkerInfo workerInfos : workerInfoList) {
        workerList.add(workerInfos.getWorkerID());
      }
    } else if (workerInfoList.size() > graph.getWorkers()) {
      for (JobMasterAPI.WorkerInfo workerInfos : workerInfoList) {
        IntStream.range(0, graph.getWorkers()).mapToObj(
            i -> workerInfos.getWorkerID()).forEachOrdered(workerList::add);
        if (workerList.size() == graph.getWorkers()) {
          break;
        }

      }
    } else {
      throw new Twister2RuntimeException("Insufficient resources to run the dataflow graph");
    }
    return workerList;
  }
}
