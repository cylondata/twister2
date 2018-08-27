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
package edu.iu.dsc.tws.tsched.batch.datalocalityaware;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ITaskScheduler;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceMapCalculation;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.TaskVertexParser;

/**
 * The data locality aware task scheduler generate the task schedule plan based on the distance
 * calculated between the worker node and the data nodes where the input data resides. Once the
 * allocation is done, it calculates the task instance ram, disk, and cpu values and
 * allocates the size of the container with required ram, disk, and cpu values.
 */
public class DataLocalityBatchTaskScheduler implements ITaskScheduler {

  private static final Logger LOG = Logger.getLogger(
      DataLocalityBatchTaskScheduler.class.getName());

  private static int taskSchedulePlanId = 0;

  private Double instanceRAM;

  private Double instanceDisk;

  private Double instanceCPU;

  private Config config;

  /**
   * This method first initialize the task instance values with default task instance ram, disk, and
   * cpu values from the task scheduler context.
   */
  @Override
  public void initialize(Config cfg) {
    this.config = cfg;
    this.instanceRAM = TaskSchedulerContext.taskInstanceRam(this.config);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(this.config);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(this.config);
  }

  /**
   * This is the base method for the data locality aware task scheduling for scheduling the batch
   * task instances. It retrieves the task vertex set of the task graph and send the set to the data
   * locality aware scheduling algorithm to allocate the batch task instances which are closer
   * to the data nodes.
   * @param graph
   * @param workerPlan
   * @return
   */
  public List<TaskSchedulePlan> scheduleBatch(DataFlowTaskGraph graph, WorkerPlan workerPlan) {

    Map<Integer, List<InstanceId>> containerInstanceMap;
    Set<Vertex> taskVertexSet = new LinkedHashSet<>(graph.getTaskVertexSet());

    List<TaskSchedulePlan> taskSchedulePlanList = new ArrayList<>();
    List<Set<Vertex>> taskVertexList = TaskVertexParser.parseVertexSet(taskVertexSet, graph);

    for (Set<Vertex> vertexSet : taskVertexList) {
      if (vertexSet.size() > 1) {
        containerInstanceMap = DataLocalityBatchScheduler.
            dataLocalityBatchSchedulingAlgo(vertexSet,
                workerPlan.getNumberOfWorkers(), workerPlan, this.config);
      } else {
        Vertex vertex = vertexSet.iterator().next();
        containerInstanceMap = DataLocalityBatchScheduler.
            dataLocalityBatchSchedulingAlgo(vertex,
                workerPlan.getNumberOfWorkers(), workerPlan, this.config);
      }

      Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();

      TaskInstanceMapCalculation instanceMapCalculation = new TaskInstanceMapCalculation(
          this.instanceRAM, this.instanceCPU, this.instanceDisk);

      Map<Integer, Map<InstanceId, Double>> instancesRamMap = instanceMapCalculation.
          getInstancesRamMapInContainer(containerInstanceMap, taskVertexSet);

      Map<Integer, Map<InstanceId, Double>> instancesDiskMap = instanceMapCalculation.
          getInstancesDiskMapInContainer(containerInstanceMap, taskVertexSet);

      Map<Integer, Map<InstanceId, Double>> instancesCPUMap = instanceMapCalculation.
          getInstancesCPUMapInContainer(containerInstanceMap, taskVertexSet);

      for (int containerId : containerInstanceMap.keySet()) {

        double containerRAMValue = TaskSchedulerContext.containerRamPadding(config);
        double containerDiskValue = TaskSchedulerContext.containerDiskPadding(config);
        double containerCpuValue = TaskSchedulerContext.containerCpuPadding(config);

        List<InstanceId> taskInstanceIds = containerInstanceMap.get(containerId);
        Map<InstanceId, TaskSchedulePlan.TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

        for (InstanceId id : taskInstanceIds) {
          double instanceRAMValue = instancesRamMap.get(containerId).get(id);
          double instanceDiskValue = instancesDiskMap.get(containerId).get(id);
          double instanceCPUValue = instancesCPUMap.get(containerId).get(id);

          Resource instanceResource = new Resource(instanceRAMValue, instanceDiskValue,
              instanceCPUValue);

          taskInstancePlanMap.put(id, new TaskSchedulePlan.TaskInstancePlan(
              id.getTaskName(), id.getTaskId(), id.getTaskIndex(), instanceResource));

          containerRAMValue += instanceRAMValue;
          containerDiskValue += instanceDiskValue;
          containerCpuValue += instanceDiskValue;
        }

        Worker worker = workerPlan.getWorker(containerId);
        Resource containerResource;

        if (worker != null && worker.getCpu() > 0 && worker.getDisk() > 0 && worker.getRam() > 0) {
          containerResource = new Resource((double) worker.getRam(),
              (double) worker.getDisk(), (double) worker.getCpu());
          LOG.fine("Worker (if loop):" + containerId + "\tRam:" + worker.getRam()
              + "\tDisk:" + worker.getDisk() + "\tCpu:" + worker.getCpu());
        } else {
          containerResource = new Resource(containerRAMValue, containerDiskValue,
              containerCpuValue);
          LOG.fine("Worker (else loop):" + containerId + "\tRam:" + containerRAMValue
              + "\tDisk:" + containerDiskValue + "\tCpu:" + containerCpuValue);
        }

        TaskSchedulePlan.ContainerPlan taskContainerPlan =
            new TaskSchedulePlan.ContainerPlan(containerId,
                new HashSet<>(taskInstancePlanMap.values()), containerResource);
        containerPlans.add(taskContainerPlan);
      }
      taskSchedulePlanList.add(new TaskSchedulePlan(taskSchedulePlanId, containerPlans));
      taskSchedulePlanId++;
    }

    //To print the schedule plan list
    for (TaskSchedulePlan taskSchedulePlan : taskSchedulePlanList) {
      Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap
          = taskSchedulePlan.getContainersMap();
      for (Map.Entry<Integer, TaskSchedulePlan.ContainerPlan> entry : containersMap.entrySet()) {
        Integer integer = entry.getKey();
        TaskSchedulePlan.ContainerPlan containerPlan = entry.getValue();
        Set<TaskSchedulePlan.TaskInstancePlan> taskContainerPlan = containerPlan.getTaskInstances();
        for (TaskSchedulePlan.TaskInstancePlan ip : taskContainerPlan) {
          LOG.fine("Task Id:" + ip.getTaskId() + "\tTask Index" + ip.getTaskIndex()
              + "\tTask Name:" + ip.getTaskName() + "\tContainer Id:" + integer);
        }
      }
    }
    return taskSchedulePlanList;
  }

  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph graph, WorkerPlan workerPlan) {
    return null;
  }
}
