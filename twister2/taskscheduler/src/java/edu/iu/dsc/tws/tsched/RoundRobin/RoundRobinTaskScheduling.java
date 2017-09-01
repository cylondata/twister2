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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

//import edu.iu.dsc.tws.tsched.spi.common.Config; (In future it will be replaced with proper config value)
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ScheduleException;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedule;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

import edu.iu.dsc.tws.tsched.utils.Job;
import edu.iu.dsc.tws.tsched.utils.JobAttributes;
import edu.iu.dsc.tws.tsched.utils.JobConfig;
import edu.iu.dsc.tws.tsched.utils.Config;

/***
 * This class is responsible for
 * 1. Initializing the RAM, Disk, and CPU percentage values from the Config and Job files.
 * 2. Perform the Round Robin based scheduling for assigning the instances to the containers.
 * 3. Generate the task schedule plan for the containers and the instances in those containers.
 */
public class RoundRobinTaskScheduling implements TaskSchedule {

  private static final Logger LOG = Logger.getLogger(RoundRobinTaskScheduling.class.getName());
  private Job job;
  private double instanceRAM;
  private double instanceDisk;
  private double instanceCPU;

  @Override
  public void initialize(Config config, Job job) {
    this.job = job;
    //This value should be modified and it should read from the job/configuration file.
    this.instanceRAM = config.Container_Max_RAM_Value;
    this.instanceCPU = config.Container_Max_CPU_Value;
    this.instanceDisk = config.Container_Max_Disk_Value;
  }

  /***
   * This method invokes the FIFO/FCFS Scheduling Method and fetch the container instance allocation map.
   * Using that map value it calculates the required ram, disk, and cpu percentage and generates the task schedule plan
   * for the instances and the containers.
   *
   * @return
   */
  @Override
  public TaskSchedulePlan tschedule() throws ScheduleException {

    Map<Integer, List<InstanceId>> containerInstanceAllocationMap = RoundRobinScheduling();
    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();

    double containerCPUValue = getContainerCPUValue(containerInstanceAllocationMap);
    double containerRAMValue = getContainerRAMValue(containerInstanceAllocationMap);
    double containerDiskValue = getContainerDiskValue(containerInstanceAllocationMap);

    for(Integer containerId:containerInstanceAllocationMap.keySet()){

        List<InstanceId> taskInstanceIds = containerInstanceAllocationMap.get(containerId);
        Map<InstanceId, TaskSchedulePlan.TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

        for(InstanceId id: taskInstanceIds) {

          double instanceCPUValue = instanceCPU;
          double instanceRAMValue = instanceRAM;
          double instanceDiskValue = instanceDisk;

          Resource resource  = new Resource (instanceRAM,instanceDisk,instanceCPU);
          taskInstancePlanMap.put(id,new TaskSchedulePlan.TaskInstancePlan("mpitask",1,1, resource));

        }
        Resource resource = new Resource(containerRAMValue, containerDiskValue, containerCPUValue);
        TaskSchedulePlan.ContainerPlan taskContainerPlan = new TaskSchedulePlan.ContainerPlan(containerId, new HashSet<>(taskInstancePlanMap.values()),resource));

        containerPlans.add(taskContainerPlan);
    }
    return new TaskSchedulePlan(job.getId(),containerPlans);
  }

  /***
  * This method is to perform the Round Robin based Scheduling operation.
  * And, it will allocate the instances in a Round Robin mode.
  *
  * @return
  */
  private Map<Integer,List<InstanceId>> RoundRobinScheduling() {

    int taskIndex = 1;
    int globalTaskIndex = 1;

    Map<Integer, List<InstanceId>> containerInstanceAllocation = new HashMap<>();

    int numberOfContainers = JobAttributes.getNumberOfContainers(job);
    int totalInstances = JobAttributes.getTotalNumberOfInstances(job);

    for(int i = 1; i <= numberOfContainers; i++) {
      containerInstanceAllocation.put(i, new ArrayList<InstanceId>());
    }

    //This value will be replaced with the actual parameters
    Map<String,Integer> parallelTaskMap = JobAttributes.getParallelTaskMap(job);
    for(String taskName : parallelTaskMap.keySet()){
      int numberOfInstances = parallelTaskMap.get(taskName);
      for(int i = 0; i < numberOfInstances; i++){
        containerInstanceAllocation.get(taskIndex).add(new InstanceId(taskName, globalTaskIndex, i));
        taskIndex = (taskIndex == numberOfContainers) ? 1 : taskIndex + 1;
        globalTaskIndex++;
      }
    }
    return containerInstanceAllocation;
  }

  @Override
  public void close() {

  }

  //These three methods will be modified with the actual values....
  private double getContainerRAMValue(Map<Integer, List<InstanceId>> containerInstanceAllocationMap) {
    //double RAMValue = Config.Container_Max_RAM_Value;
    double containerRAMValue = 0.0;
    try {
      containerRAMValue = Double.valueOf(JobConfig.Container_Max_RAM_Value.trim());

    } catch (Exception exception) {
      exception.printStackTrace();
    }
    return containerRAMValue;
  }

  private double getContainerCPUValue(Map<Integer, List<InstanceId>> containerInstanceAllocationMap) {
    //double CPUValue = Config.Container_Max_CPU_Value;
    double containerCPUValue = 0.0;
    try {
      containerCPUValue = Double.valueOf(JobConfig.Container_Max_CPU_Value.trim());
    } catch (Exception exception) {
      exception.printStackTrace();
    }
    return containerCPUValue;
  }

  private double getContainerDiskValue(Map<Integer, List<InstanceId>> containerInstanceAllocationMap) {
    //double DiskValue = Config.Container_Max_Disk_Value;
    double containerDiskValue = 0.0;
    try {
      containerDiskValue = Double.valueOf(JobConfig.Container_Max_Disk_Value.trim());
    } catch (Exception exception) {
      exception.printStackTrace();
    }
    return containerDiskValue;
  }

}
