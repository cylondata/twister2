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
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceMapCalculation;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedule;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class RoundRobinTaskScheduling implements TaskSchedule {

  private static final Logger LOG = Logger.getLogger(RoundRobinTaskScheduling.class.getName());

  private static final double DEFAULT_DISK_PADDING_PER_CONTAINER = 12;
  private static final double DEFAULT_CPU_PADDING_PER_CONTAINER = 1;
  private static final double DEFAULT_RAM_PADDING_PER_CONTAINER = 2;

  private Double instanceRAM;
  private Double instanceDisk;
  private Double instanceCPU;

  private Resource containerMaximumResource;
  private Resource defaultResource;
  private Config cfg;

  @Override
  public void initialize(Config cfg1) {
    this.cfg = cfg1;
    this.instanceRAM = 1024.0;
    this.instanceDisk = 100.0;
    this.instanceCPU = 2.0;

    //Retrieve the default instance values from the config file.
    /*this.instanceRAM = Double.parseDouble(cfg.getStringValue("INSTANCE_RAM"));
    this.instanceDisk = Double.parseDouble(cfg.getStringValue("INSTANCE_DISK"));
    this.instanceCPU = Double.parseDouble(cfg.getStringValue("INSTANCE_CPU"));*/

    //The commented value should be enabled once the context class is created.
    /*this.instanceRAM = Context.instanceRam(cfg);
    this.instanceDisk = Context.instanceDisk(cfg);
    this.instanceCPU = Context.instanceCPU(cfg);*/
  }

  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {

    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();
    Set<Vertex> taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();

    Map<Integer, List<InstanceId>> roundRobinContainerInstanceMap =
        RoundRobinScheduling.RoundRobinSchedulingAlgorithm(taskVertexSet,
            workerPlan.getNumberOfWorkers());

    TaskInstanceMapCalculation instanceMapCalculation = new TaskInstanceMapCalculation(
        instanceRAM, instanceCPU, instanceDisk);

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

      AtomicReference<Double> containerRAMValue =
          new AtomicReference<>(DEFAULT_RAM_PADDING_PER_CONTAINER);
      AtomicReference<Double> containerDiskValue =
          new AtomicReference<>(DEFAULT_DISK_PADDING_PER_CONTAINER);
      AtomicReference<Double> containerCPUValue =
          new AtomicReference<>(DEFAULT_CPU_PADDING_PER_CONTAINER);

      List<InstanceId> taskInstanceIds = roundRobinContainerInstanceMap.get(containerId);
      Map<InstanceId, TaskSchedulePlan.TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

      for (InstanceId id : taskInstanceIds) {
        double instanceRAMValue = instancesRamMap.get(containerId).get(id);
        double instanceDiskValue = instancesDiskMap.get(containerId).get(id);
        double instanceCPUValue = instancesCPUMap.get(containerId).get(id);

        LOG.info(String.format("Task Id and Index\t" + id.getTaskId() + "\t" + id.getTaskIndex()
            + "\tand Required Resource:" + instanceRAMValue + "\t"
            + instanceDiskValue + "\t" + instanceCPUValue));

        Resource instanceResource = new Resource(instanceRAMValue,
            instanceDiskValue, instanceCPUValue);

        taskInstancePlanMap.put(id, new TaskSchedulePlan.TaskInstancePlan(
                id.getTaskName(), id.getTaskId(), id.getTaskIndex(), instanceResource));

        containerRAMValue.updateAndGet(v -> v + instanceRAMValue);
        containerDiskValue.updateAndGet(v -> v + instanceDiskValue);
        containerCPUValue.updateAndGet(v -> v + instanceCPUValue);
      }

      LOG.info(String.format("Container id:" + containerId
          + "\tand allocated resource values\t"
          + "ram:" + containerRAMValue.get() + "\t"
          + "disk:" + containerCPUValue.get() + "\t"
          + "cpu:" + containerCPUValue.get()));

      Worker worker = workerPlan.getWorker(containerId);

      LOG.info(String.format("Worker values:" + workerPlan.getNumberOfWorkers()
          + "\tRam:" + worker.getRam() + "\tDisk:" + worker.getDisk()
          + "\tCpu:" + worker.getCpu()));

      Resource containerResource = new Resource((double) worker.getRam(),
          (double) worker.getDisk(), (double) worker.getCpu());

      //Perfectly Working Condition
      /*Resource containerResource = new Resource(
          containerRAMValue, containerDiskValue, containerCPUValue);*/

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
