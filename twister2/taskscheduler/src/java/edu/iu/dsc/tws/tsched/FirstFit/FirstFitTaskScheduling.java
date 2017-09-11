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
package edu.iu.dsc.tws.tsched.FirstFit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.tsched.spi.common.Config;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedule;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.Job;
import edu.iu.dsc.tws.tsched.utils.JobAttributes;
import edu.iu.dsc.tws.tsched.utils.JobConfig;

//import edu.iu.dsc.tws.tsched.spi.common.Config; (In future it will be replaced with proper job config values)

/***
 * This class is responsible for
 * 1. Initializing the RAM, Disk, and CPU percentage values from the Job and its configuration files.
 * 2. Perform the FCFS scheduling for assigning the instances to the containers.
 * 3. Generate the task schedule plan for the containers and the instances (tasks) in those containers.
 */

public class FirstFitTaskScheduling implements TaskSchedule {

  private static final Logger LOG = Logger.getLogger(FirstFitTaskScheduling.class.getName());

  private Job job;
  private double instanceRAM;
  private double instanceDisk;
  private double instanceCPU;

  //It should be replaced with appropriate values....
  private static final double DEFAULT_DISK_PADDING_PER_CONTAINER = 10;
  private static final double DEFAULT_CPU_PADDING_PER_CONTAINER = 1;
  private static final double MIN_RAM_PER_INSTANCE = 248;
  private static final double NOT_SPECIFIED_NUMBER_VALUE = -1;
  private static final double DEFAULT_RAM_PADDING_PER_CONTAINER = 2;

  //This value should be modified and it should read from the job/configuration file.
  @Override
  public void initialize(Config config, Job job) {
    this.job = job;
    this.instanceRAM = config.Container_Max_RAM_Value;
    this.instanceCPU = config.Container_Max_CPU_Value;
    this.instanceDisk = config.Container_Max_Disk_Value;
  }

  /***
   * This method invokes the First Fit Scheduling Method and fetch the container instance allocation map.
   * Using the map value it calculates the required ram, disk, and cpu percentage for
   * each container and instances in each container and generates the task schedule plan
   * for those instances and the containers.
   *
   */
  @Override
  public TaskSchedulePlan tschedule() {

    Map<Integer, List<InstanceId>> containerInstanceAllocationMap = FirstFitScheduling();
    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();

    double containerCPUValue = getContainerCPUValue(containerInstanceAllocationMap);
    double containerRAMValue = getContainerRamValue(containerInstanceAllocationMap);
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
      TaskSchedulePlan.ContainerPlan taskContainerPlan = new TaskSchedulePlan.ContainerPlan(containerId, new HashSet<>(taskInstancePlanMap.values()),resource);

      containerPlans.add(taskContainerPlan);
    }
    return new TaskSchedulePlan(job.getId(), containerPlans);
  }

  /***
   * This method performs the actual First Fit task scheduling operation.
   *
   * @return
   */
  private Map<Integer,List<InstanceId>> FirstFitScheduling() {

    int taskIndex = 1;
    int globalTaskIndex = 1;
    Job job = new Job();
    job = job.getJob();
    Map<Integer, List<InstanceId>> FirstFitScheduleMap = new HashMap<>();

    try {
      int numberOfContainers = JobAttributes.getNumberOfContainers(job);
      int totalInstances = JobAttributes.getTotalNumberOfInstances(job);

      for (int i = 1; i <= numberOfContainers; i++) {
        FirstFitScheduleMap.put(i, new ArrayList<InstanceId>());
      }

      //This value will be replaced with the actual parameters
      Map<String, Integer> parallelTaskMap = JobAttributes.getParallelTaskMap(job);
      int size = FirstFitScheduleMap.size ();


      for(int index = 1; index <= size; index++){
        for (String task : parallelTaskMap.keySet()) {
          int numberOfInstances = parallelTaskMap.get (task);
          for (int i = 0; i < numberOfInstances; i++) {
            FirstFitScheduleMap.get(index).add(new InstanceId (task, globalTaskIndex,i));
            //FirstFitScheduleMap.add(new InstanceId (task,globalTaskIndex,index));
            //FirstFitScheduleMap.get (index).add (new InstanceId (task, globalTaskIndex, index));
            //FirstFitScheduleMap.get (taskIndex).add (new InstanceId (task, globalTaskIndex, i));
            //taskIndex = (taskIndex == numberOfContainers) ? 1 : taskIndex + 1;
            //globalTaskIndex++;
            //taskIndex = taskIndex + 1;
            globalTaskIndex += 1;
          }
        }
      }
    }
    catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return FirstFitScheduleMap;
  }

  @Override
  public void close() {

  }

  private Map<Integer, Map<InstanceId, Double>> getInstancesRamMapInContainer(
      Map<Integer, List<InstanceId>> containerInstanceAllocationMap) {

    Map<String, Double> ramMap = JobAttributes.getComponentRAMMapConfig();
    Map<Integer, Map<InstanceId, Double>> InstancesRamContainerMap = new HashMap<> ();

    for (int containerId : containerInstanceAllocationMap.keySet ()) {

      Double usedRamValue = 0.0;
      List<InstanceId> instanceIds = containerInstanceAllocationMap.get (containerId);
      Map<InstanceId, Double> containerRam = new HashMap<> ();
      InstancesRamContainerMap.put (containerId, containerRam);
      List<InstanceId> instancesToBeCalculated = new ArrayList<> ();

      for (InstanceId instanceId : instanceIds) {
        String taskName = instanceId.getTaskName ();
        if (ramMap.containsKey (taskName)) {
          Double ramValue = ramMap.get (taskName);
          containerRam.put (instanceId, ramValue);
        } else {
          instancesToBeCalculated.add (instanceId);
        }
      }

      Double containerRamValue = getContainerRamValue (containerInstanceAllocationMap);
      int instancesAllocationSize = instancesToBeCalculated.size ();
      if (instancesAllocationSize != 0) {
        Double instanceRequiredRam = instanceRAM;

        if(!containerRamValue.equals (NOT_SPECIFIED_NUMBER_VALUE)){
          Double remainingRam = containerRamValue - DEFAULT_RAM_PADDING_PER_CONTAINER - usedRamValue;
          instanceRequiredRam = remainingRam / instancesAllocationSize;

        }
        for (InstanceId instanceId : instancesToBeCalculated) {
          containerRam.put(instanceId, instanceRequiredRam);
        }
      }
    }
    return InstancesRamContainerMap;
  }

  private Map<Integer, Map<InstanceId, Double>> getInstancesDiskMapInContainer(
      Map<Integer, List<InstanceId>> containerInstanceAllocationMap) {

    Map<String, Double> diskMap = JobAttributes.getComponentDiskMapConfig ();
    Map<Integer, Map<InstanceId, Double>> InstancesDiskContainerMap = new HashMap<> ();

    for (int containerId : containerInstanceAllocationMap.keySet ()) {
      Double usedDiskValue = 0.0;
      List<InstanceId> instanceIds = containerInstanceAllocationMap.get (containerId);
      Map<InstanceId, Double> containerDisk = new HashMap<> ();
      InstancesDiskContainerMap.put (containerId, containerDisk );
      List<InstanceId> instancesToBeCalculated = new ArrayList<> ();

      for (InstanceId instanceId : instanceIds) {
        String taskName = instanceId.getTaskName ();

        if (diskMap.containsKey (taskName)) {
          Double diskValue = diskMap.get (taskName);
          containerDisk .put (instanceId, diskValue);
        } else {
          instancesToBeCalculated.add (instanceId);
        }
      }

      Double containerDiskValue = getContainerDiskValue (containerInstanceAllocationMap);
      int instancesAllocationSize = instancesToBeCalculated.size ();
      if (instancesAllocationSize != 0) {
        Double instanceRequiredDisk = 0.0;
        if(!containerDiskValue.equals (NOT_SPECIFIED_NUMBER_VALUE)){
          Double remainingDisk = containerDiskValue - DEFAULT_DISK_PADDING_PER_CONTAINER - usedDiskValue;
          instanceRequiredDisk = remainingDisk / instancesAllocationSize;
        }
        for (InstanceId instanceId : instancesToBeCalculated) {
          containerDisk .put(instanceId, instanceRequiredDisk);
        }
        System.out.println("Instances Required Disk:\t"+instanceRequiredDisk);
      }
    }
    return InstancesDiskContainerMap;

  }
  private Double getContainerCPUValue(Map<Integer, List<InstanceId>> InstancesAllocation) {

        /*List<JobAPI.Config.KeyValue> jobConfig= job.getJobConfig().getKvsList();
        double defaultContainerCpu =
                DEFAULT_CPU_PADDING_PER_CONTAINER + getLargestContainerSize(InstancesAllocation);

        String cpuHint = JobUtils.getConfigWithDefault(
                jobConfig, com.tws.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED,
                Double.toString(defaultContainerCpu)); */

    //These two lines will be removed with the above commented code, once the actual job description file is created...
    String cpuHint = "0.6";
    return Double.parseDouble(cpuHint);
  }

  private Double getContainerDiskValue(Map<Integer, List<InstanceId>> InstancesAllocation) {

        /*ByteAmount defaultContainerDisk = instanceDiskDefault
                .multiply(getLargestContainerSize(InstancesAllocation))
                .plus(DEFAULT_DISK_PADDING_PER_CONTAINER);

        List<JobAPI.Config.KeyValue> jobConfig = job.getJobConfig().getKvsList();
        return JobUtils.getConfigWithDefault(jobConfig,
                com.tws.api.Config.JOB_CONTAINER_DISK_REQUESTED,
                defaultContainerDisk); */
    //return ByteAmount.fromGigabytes (containerDiskValue);

    //These two lines will be removed with the above commented code, once the actual job description file is created...
    Long containerDiskValue = 10L;
    return containerDiskValue.doubleValue();
  }

  private Double getContainerRamValue(Map<Integer, List<InstanceId>> InstancesAllocation) {

        /*List<JobAPI.Config.KeyValue> jobConfig = job.getJobConfig().getKvsList();
        return JobUtils.getConfigWithDefault(
                jobConfig, com.tws.api.Config.JOB_CONTAINER_RAM_REQUESTED,
                NOT_SPECIFIED_NUMBER_VALUE); */

    //These two lines will be removed with the above commented code, once the actual job description file is created...
    Long containerRAMValue = 10L;
    return containerRAMValue.doubleValue();
  }

}
