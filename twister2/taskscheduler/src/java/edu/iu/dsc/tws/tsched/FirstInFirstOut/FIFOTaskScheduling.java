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
package edu.iu.dsc.tws.tsched.FirstInFirstOut;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.tsched.spi.common.Config;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ScheduleException;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedule;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.Job;
import edu.iu.dsc.tws.tsched.utils.JobAttributes;


public class FIFOTaskScheduling implements TaskSchedule {

  private static final Logger LOG = Logger.getLogger(FIFOTaskScheduling.class.getName());

  @Override
  public void initialize(Config config, Job job) {
    this.job = job;
    //This value should be modified and it should read from the job/configuration file.
    this.instanceRAM = config.Container_Max_RAM_Value;
    this.instanceCPU = config.Container_Max_CPU_Value;
    this.instanceDisk = config.Container_Max_Disk_Value;
  }

  @Override
  public TaskSchedulePlan tschedule() throws ScheduleException {

    Map<Integer, List<InstanceId>> FIFOAllocation = doFIFOAllocation();

    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();

    double containerCPU = getContainerCPUValue(FIFOAllocation);
    double containerRAM = getContainerRAMRequested(FIFOAllocation);
    double containerDisk = getContainerDiskRequested(FIFOAllocation);

    for(Integer containerId:FIFOAllocation.keySet()){

      List<InstanceId> taskInstanceIds = FIFOAllocation.get(containerId);
      Map<InstanceId, TaskSchedulePlan.TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

      for(InstanceId id: taskInstanceIds) {

        double instanceCPUValue = instanceCPU;
        double instanceRAMValue = instanceRAM;
        double instanceDiskValue = instanceDisk;

        Resource resource  = new Resource (instanceRAM,instanceDisk,instanceCPU);
        taskInstancePlanMap.put(id,new TaskSchedulePlan.TaskInstancePlan("mpitask",1,1, resource));

      }
      Resource resource = new Resource(containerRAM, containerDisk, containerCPU);
      TaskSchedulePlan.ContainerPlan = new TaskSchedulePlan.ContainerPlan(containerId, taskInstancePlanMap.values(),resource));
    }
    return new TaskSchedulePlan(job.getId(),containerPlans);
  }

  private Map<Integer,List<InstanceId>> doFIFOAllocation (){

    Map<Integer, List<InstanceId>> FIFOAllocation = new HashMap<>();

    //This value will be replaced with the actual parameters
    int numberOfContainers = JobAttributes.getNumberOfContainers(job);
    int totalInstances = JobAttributes.getTotalNumberOfInstances(job);
    ///////////////////////////////////////////////////////////

    for(int i = 1; i <= numberOfContainers; i++) {
      FIFOAllocation.put(i, new ArrayList<InstanceId>());
    }

    int taskIndex = 1;
    int globalTaskIndex = 1;

    //This value will be replaced with the actual parameters
    Map<String,Integer> parallelTaskMap = JobAttributes.getParallelTaskMap(job);

    //This logic should be replaced with FIFO logic....
    for(String task : parallelTaskMap.keySet()){
      int numberOfInstances = parallelTaskMap.get(task);
      for(int i = 0; i < numberOfInstances; i++){
        FIFOAllocation.get(taskIndex).add(new InstanceId(task, globalTaskIndex, i));
        taskIndex = (taskIndex == numberOfContainers) ? 1 : taskIndex + 1;
        globalTaskIndex++;
      }
    }
    return FIFOAllocation;
  }

  @Override
  public void close() {

  }

  //These three methods will be modified with the actual values....
  private double getContainerRAMRequested(Map<Integer, List<InstanceId>> roundRobinAllocation) {
    double RAMValue = Config.Container_Max_RAM_Value;
    return RAMValue;
  }

  private double getContainerCPUValue(Map<Integer, List<InstanceId>> roundRobinAllocation) {
    double CPUValue = Config.Container_Max_CPU_Value;
    return CPUValue;
  }

  private double getContainerDiskRequested(Map<Integer, List<InstanceId>> roundRobinAllocation) {
    double DiskValue = Config.Container_Max_Disk_Value;
    return DiskValue;
  }

  private class InstanceId {

    private final String taskName;
    private final int taskId;
    private final int taskIndex;

    public InstanceId(String taskName, int taskId, int taskIndex) {
      this.taskId = taskId;
      this.taskName = taskName;
      this.taskIndex = taskIndex;
    }
    public String getTaskName() {
      return taskName;
    }
    public int getTaskId() {
      return taskId;
    }
    public int getTaskIndex() {
      return taskIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof InstanceId)) return false;

      InstanceId that = (InstanceId) o;

      if (taskId != that.taskId) return false;
      if (taskIndex != that.taskIndex) return false;
      return taskName != null ? taskName.equals(that.taskName) : that.taskName == null;
    }

    @Override
    public int hashCode() {
      int result = taskName != null ? taskName.hashCode() : 0;
      result = 31 * result + taskId;
      result = 31 * result + taskIndex;
      return result;
    }
  }

}
