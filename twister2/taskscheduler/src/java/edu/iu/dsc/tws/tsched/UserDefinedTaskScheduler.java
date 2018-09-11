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
package edu.iu.dsc.tws.tsched;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;

/**
 * This class allocate the task instances into the container in a round robin manner. First, it will
 * allocate the task instances into the logical container values and then it will calculate the
 * required ram, disk, and cpu values for task instances and the containers which is based
 * on the task configuration values and the allocated worker values respectively.
 * <p>
 * For example, if there are two tasks with parallelism value of 2, 1st task -> instance 0 will
 * go to container 0, 2nd task -> instance 0 will go to container 1, 1st task -> instance 1 will
 * go to container 1, 2nd task -> instance 1 will go to container 1.
 */
public class UserDefinedTaskScheduler implements ITaskScheduler {

  private static final Logger LOG = Logger.getLogger(UserDefinedTaskScheduler.class.getName());

  //Represents the task schedule plan Id
  private static int taskSchedulePlanId = 0;

  //Represents the task instance ram
  private Double instanceRAM;

  //Represents the task instance disk
  private Double instanceDisk;

  //Represents the task instance cpu value
  private Double instanceCPU;

  //Config object
  private Config config;

  /**
   * This method retrieves the parallel task map and the total number of task instances for the task
   * vertex set. Then, it will allocate the instances into the number of containers allocated for
   * the task in a round robin fashion.
   */
  private static Map<Integer, List<InstanceId>> userDefinedSchedulingAlgorithm(
          Set<Vertex> taskVertexSet, int numberOfContainers) {

    TaskAttributes taskAttributes = new TaskAttributes();
    Map<Integer, List<InstanceId>> roundrobinAllocation = new LinkedHashMap<>();

    for (int i = 0; i < numberOfContainers; i++) {
      roundrobinAllocation.put(i, new ArrayList<>());
    }

    try {
      Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
      int totalTaskInstances = taskAttributes.getTotalNumberOfInstances(taskVertexSet);
      if (numberOfContainers < totalTaskInstances) {
        int globalTaskIndex = 0;
        for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
          String task = e.getKey();
          int numberOfInstances = e.getValue();
          int containerIndex;
          for (int i = 0; i < numberOfInstances; i++) {
            containerIndex = i % numberOfContainers;
            roundrobinAllocation.get(containerIndex).add(new InstanceId(task, globalTaskIndex, i));
          }
          globalTaskIndex++;
        }
      }

      //To print the allocation map
      for (Map.Entry<Integer, List<InstanceId>> entry : roundrobinAllocation.entrySet()) {
        Integer integer = entry.getKey();
        List<InstanceId> instanceIds = entry.getValue();
        LOG.fine("Container Index:" + integer);
        for (InstanceId instanceId : instanceIds) {
          LOG.fine("Task Instance Details:"
                  + "\t Task Name:" + instanceId.getTaskName()
                  + "\t Task id:" + instanceId.getTaskId()
                  + "\t Task index:" + instanceId.getTaskIndex());
        }
      }
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return roundrobinAllocation;
  }

  /**
   * This method initialize the task instance values with the values specified in the task config
   * object.
   *
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
   * This is the base method which receives the dataflow taskgraph and the worker plan to allocate
   * the task instances to the appropriate workers with their required ram, disk, and cpu values.
   *
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

    //Allocate the task instances into the logical containers.
    Map<Integer, List<InstanceId>> roundRobinContainerInstanceMap =
            userDefinedSchedulingAlgorithm(taskVertexSet, workerPlan.getNumberOfWorkers());

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

        Resource instanceResource = new Resource(instanceRAMValue,
                instanceDiskValue, instanceCPUValue);

        taskInstancePlanMap.put(id, new TaskSchedulePlan.TaskInstancePlan(id.getTaskName(),
                id.getTaskId(), id.getTaskIndex(), instanceResource));

        containerRAMValue += instanceRAMValue;
        containerDiskValue += instanceDiskValue;
        containerCpuValue += instanceDiskValue;
      }

      Worker worker = workerPlan.getWorker(containerId);
      Resource containerResource;

      /*Create the container resource value which is based on the worker values received from the
      worker plan */
      if (worker != null && worker.getCpu() > 0 && worker.getDisk() > 0 && worker.getRam() > 0) {
        containerResource = new Resource((double) worker.getRam(),
                (double) worker.getDisk(), (double) worker.getCpu());
        LOG.fine("Worker (if loop):" + containerId + "\tRam:" + worker.getRam()
                + "\tDisk:" + worker.getDisk() + "\tCpu:" + worker.getCpu());
      } else {
        containerResource = new Resource(containerRAMValue, containerDiskValue, containerCpuValue);
        LOG.fine("Worker (else loop):" + containerId + "\tRam:" + containerRAMValue
                + "\tDisk:" + containerDiskValue + "\tCpu:" + containerCpuValue);
      }

      //Schedule the task instance plan into the task container plan.
      TaskSchedulePlan.ContainerPlan taskContainerPlan =
              new TaskSchedulePlan.ContainerPlan(containerId,
                      new HashSet<>(taskInstancePlanMap.values()), containerResource);
      containerPlans.add(taskContainerPlan);
    }

    //In future, taskSchedulePlanId will be replaced with the JobId or Task Graph Id.
    return new TaskSchedulePlan(taskSchedulePlanId, containerPlans);
  }
}

