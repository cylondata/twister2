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
package edu.iu.dsc.tws.tsched.streaming.datalocalityaware;

import java.util.HashMap;
import java.util.HashSet;
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

public class DataLocalityStreamingTaskScheduler implements ITaskScheduler {

  private static final Logger LOG = Logger.getLogger(
      DataLocalityStreamingTaskScheduler.class.getName());

  private static int taskSchedulePlanId = 0;
  private Double instanceRAM;
  private Double instanceDisk;
  private Double instanceCPU;
  private Config cfg;

  @Override
  public void initialize(Config cfg1) {
    this.cfg = cfg1;
    this.instanceRAM = TaskSchedulerContext.taskInstanceRam(cfg);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(cfg);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(cfg);
  }

  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph graph, WorkerPlan workerPlan) {

    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();
    //Set<Vertex> taskVertexSet = new LinkedHashSet<>(graph.getTaskVertexSet());
    Set<Vertex> taskVertexSet = graph.getTaskVertexSet();

    Map<Integer, List<InstanceId>> containerInstanceMap =
        DataLocalityStreamingScheduling.dataLocalityAwareSchedulingAlgorithm(taskVertexSet,
            workerPlan.getNumberOfWorkers(), workerPlan, this.cfg);

    TaskInstanceMapCalculation instanceMapCalculation = new TaskInstanceMapCalculation(
        this.instanceRAM, this.instanceCPU, this.instanceDisk);

    Map<Integer, Map<InstanceId, Double>> instancesRamMap =
        instanceMapCalculation.getInstancesRamMapInContainer(containerInstanceMap,
            taskVertexSet);

    Map<Integer, Map<InstanceId, Double>> instancesDiskMap =
        instanceMapCalculation.getInstancesDiskMapInContainer(containerInstanceMap,
            taskVertexSet);

    Map<Integer, Map<InstanceId, Double>> instancesCPUMap =
        instanceMapCalculation.getInstancesCPUMapInContainer(containerInstanceMap,
            taskVertexSet);

    for (int containerId : containerInstanceMap.keySet()) {

      Double containerRAMValue = TaskSchedulerContext.containerRamPadding(cfg);
      Double containerDiskValue = TaskSchedulerContext.containerDiskPadding(cfg);
      Double containerCpuValue = TaskSchedulerContext.containerCpuPadding(cfg);

      List<InstanceId> taskInstanceIds = containerInstanceMap.get(containerId);
      Map<InstanceId, TaskSchedulePlan.TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

      for (InstanceId id : taskInstanceIds) {
        double instanceRAMValue = instancesRamMap.get(containerId).get(id);
        double instanceDiskValue = instancesDiskMap.get(containerId).get(id);
        double instanceCPUValue = instancesCPUMap.get(containerId).get(id);

        Resource instanceResource = new Resource(instanceRAMValue,
            instanceDiskValue, instanceCPUValue);

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
        //write into a log file
        LOG.fine("Worker (if loop):" + containerId + "\tRam:"
            + worker.getRam() + "\tDisk:" + worker.getDisk()  //write into a log file
            + "\tCpu:" + worker.getCpu());
      } else {
        containerResource = new Resource(containerRAMValue, containerDiskValue,
            containerCpuValue);
        //write into a log file
        LOG.fine("Worker (else loop):" + containerId
            + "\tRam:" + containerRAMValue     //write into a log file
            + "\tDisk:" + containerDiskValue
            + "\tCpu:" + containerCpuValue);
      }

      TaskSchedulePlan.ContainerPlan taskContainerPlan =
          new TaskSchedulePlan.ContainerPlan(containerId,
              new HashSet<>(taskInstancePlanMap.values()), containerResource);
      containerPlans.add(taskContainerPlan);
    }
    return new TaskSchedulePlan(taskSchedulePlanId, containerPlans);
  }
}
