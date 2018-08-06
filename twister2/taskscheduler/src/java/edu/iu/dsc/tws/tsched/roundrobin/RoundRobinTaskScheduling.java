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
package edu.iu.dsc.tws.tsched.roundrobin;

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
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ScheduleException;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceMapCalculation;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedule;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class RoundRobinTaskScheduling implements TaskSchedule {

  private static final Logger LOG = Logger.getLogger(RoundRobinTaskScheduling.class.getName());

  private static int taskSchedulePlanId = 0;

  //It represents the task instance ram
  private Double instanceRAM;

  //It represents the task instance disk
  private Double instanceDisk;

  //It represents the task instance cpu value
  private Double instanceCPU;

  private Config config;

  /**
   * This method initialize the task instance values with the values specified in
   * the config object.
   * @param cfg
   */
  @Override
  public void initialize(Config cfg) {
    this.config = cfg;
    this.instanceRAM = TaskSchedulerContext.taskInstanceRam(config);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(config);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(config);
  }

  /**
   * This is the base method which receives the dataflow taskgraph and workerplan
   * to allocate the task instances to the appropriate workers.
   * @param dataFlowTaskGraph
   * @param workerPlan
   * @return
   */

  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {

    //Allocate the task instances into the containers/workers
    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new LinkedHashSet<>();

    //To get the vertex set from the taskgraph
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

      double containerRAMValue = TaskSchedulerContext.containerRamPadding(config);
      double containerDiskValue = TaskSchedulerContext.containerDiskPadding(config);
      double containerCpuValue = TaskSchedulerContext.containerCpuPadding(config);

      List<InstanceId> taskInstanceIds = roundRobinContainerInstanceMap.get(containerId);
      Map<InstanceId, TaskSchedulePlan.TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

      for (InstanceId id : taskInstanceIds) {
        double instanceRAMValue = instancesRamMap.get(containerId).get(id);
        double instanceDiskValue = instancesDiskMap.get(containerId).get(id);
        double instanceCPUValue = instancesCPUMap.get(containerId).get(id);

        /*LOG.info(String.format("Task Id and Index\t" + id.getTaskId() + "\t" + id.getTaskIndex()
            + "\tand Required Resource:" + instanceRAMValue + "\t" //write into a log file
            + instanceDiskValue + "\t" + instanceCPUValue));*/

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
        LOG.fine("Worker (if loop):" + containerId + "\tRam:"
            + worker.getRam() + "\tDisk:" + worker.getDisk()  //write into a log file
            + "\tCpu:" + worker.getCpu());
      } else {
        containerResource = new Resource(containerRAMValue, containerDiskValue,
            containerCpuValue);
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
    //new TaskSchedulePlan(job.getJobId(), containerPlans);
    return new TaskSchedulePlan(taskSchedulePlanId, containerPlans);
  }

  @Override
  public TaskSchedulePlan tschedule() throws ScheduleException {
    return null;
  }

  @Override
  public void close() {
  }
}
