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
package edu.iu.dsc.tws.tsched.RoundRobin;

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
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceMapCalculation;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedule;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

//import org.apache.commons.lang3.ObjectUtils;
//import java.util.concurrent.atomic.AtomicReference;

public class RoundRobinTaskScheduling implements TaskSchedule {

  private static final Logger LOG = Logger.getLogger(RoundRobinTaskScheduling.class.getName());

  private Double instanceRAM;
  private Double instanceDisk;
  private Double instanceCPU;
  private Resource containerMaximumResource;
  private Resource defaultResource;
  private Config cfg;

  @Override
  public void initialize(Config cfg1) {
    this.cfg = cfg1;
    this.instanceRAM = TaskSchedulerContext.taskInstanceRam(cfg);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(cfg);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(cfg);

    LOG.info("*******************" + TaskSchedulerContext.defaultTaskInstancesPerContainer(cfg));
  }

  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {

    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();
    Set<Vertex> taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();

    Map<Integer, List<InstanceId>> roundRobinContainerInstanceMap =
        RoundRobinScheduling.RoundRobinSchedulingAlgorithm(taskVertexSet,
            workerPlan.getNumberOfWorkers());

    TaskInstanceMapCalculation instanceMapCalculation = new TaskInstanceMapCalculation(
        this.instanceRAM, this.instanceCPU, this.instanceDisk);

    Map<Integer, Map<InstanceId, Double>> instancesRamMap =
        instanceMapCalculation.getInstancesRamMapInContainer(roundRobinContainerInstanceMap,
            taskVertexSet);

    Map<Integer, Map<InstanceId, Double>> instancesDiskMap =
        instanceMapCalculation.getInstancesDiskMapInContainer(roundRobinContainerInstanceMap,
            taskVertexSet);

    Map<Integer, Map<InstanceId, Double>> instancesCPUMap =
        instanceMapCalculation.getInstancesCPUMapInContainer(roundRobinContainerInstanceMap,
            taskVertexSet);

    for (int containerId : roundRobinContainerInstanceMap.keySet()) {
      LOG.info("Container Id::::" + containerId);
      /*AtomicReference<Double> containerRAMValue =
          new AtomicReference<>(DEFAULT_RAM_PADDING_PER_CONTAINER);
      AtomicReference<Double> containerDiskValue =
          new AtomicReference<>(DEFAULT_DISK_PADDING_PER_CONTAINER);
      AtomicReference<Double> containerCPUValue =
          new AtomicReference<>(DEFAULT_CPU_PADDING_PER_CONTAINER);*/

      /*Double containerRAMValue = TaskSchedulerContext.containerInstanceRam(cfg);
      Double containerDiskValue = TaskSchedulerContext.containerInstanceDisk(cfg);
      Double containerCpuValue = TaskSchedulerContext.containerInstanceCpu(cfg);*/

      Double containerRAMValue = TaskSchedulerContext.containerRamPadding(cfg);
      Double containerDiskValue = TaskSchedulerContext.containerDiskPadding(cfg);
      Double containerCpuValue = TaskSchedulerContext.containerCpuPadding(cfg);

      List<InstanceId> taskInstanceIds = roundRobinContainerInstanceMap.get(containerId);
      Map<InstanceId, TaskSchedulePlan.TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

      for (InstanceId id : taskInstanceIds) {
        double instanceRAMValue = instancesRamMap.get(containerId).get(id);
        double instanceDiskValue = instancesDiskMap.get(containerId).get(id);
        double instanceCPUValue = instancesCPUMap.get(containerId).get(id);

        LOG.info(String.format("Task Id and Index\t" + id.getTaskId() + "\t" + id.getTaskIndex()
            + "\tand Required Resource:" + instanceRAMValue + "\t" //write into a log file
            + instanceDiskValue + "\t" + instanceCPUValue));

        Resource instanceResource = new Resource(instanceRAMValue,
            instanceDiskValue, instanceCPUValue);

        taskInstancePlanMap.put(id, new TaskSchedulePlan.TaskInstancePlan(
                id.getTaskName(), id.getTaskId(), id.getTaskIndex(), instanceResource));

        /*containerRAMValue.updateAndGet(v -> v + instanceRAMValue);
        containerDiskValue.updateAndGet(v -> v + instanceDiskValue);
        containerCPUValue.updateAndGet(v -> v + instanceCPUValue);*/

        containerRAMValue += instanceRAMValue;
        containerDiskValue += instanceDiskValue;
        containerCpuValue += instanceDiskValue;
      }

      Worker worker = workerPlan.getWorker(containerId);
      Resource containerResource;

      if (worker != null && worker.getCpu() > 0 && worker.getDisk() > 0 && worker.getRam() > 0) {
        containerResource = new Resource((double) worker.getRam(),
            (double) worker.getDisk(), (double) worker.getCpu());
        /*LOG.info(String.format("Worker (if loop):" + containerId + "\tRam:"
            + worker.getRam() + "\tDisk:" + worker.getDisk()  //write into a log file
            + "\tCpu:" + worker.getCpu()));*/
      } else {
        containerResource = new Resource(containerRAMValue, containerDiskValue,
            containerCpuValue);
        /*LOG.info(String.format("Worker (else loop):" + containerId
            + "\tRam:" + containerRAMValue     //write into a log file
            + "\tDisk:" + containerDiskValue
            + "\tCpu:" + containerCpuValue));*/
      }

      TaskSchedulePlan.ContainerPlan taskContainerPlan =
          new TaskSchedulePlan.ContainerPlan(containerId,
              new HashSet<>(taskInstancePlanMap.values()), containerResource);
      containerPlans.add(taskContainerPlan);
    }
    //return new TaskSchedulePlan(job.getJobId(), containerPlans);
    return new TaskSchedulePlan(1, containerPlans); //id would be taskgraphid/jobid
  }

  @Override
  public TaskSchedulePlan tschedule() {
    return null;
  }

  @Override
  public void close() {
  }
}
