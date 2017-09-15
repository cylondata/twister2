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
package edu.iu.dsc.tws.tsched.builder;

import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedule;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public  class TaskSchedulePlanBuilder {

  private int jobId;
  private int numNodes;
  private ArrayList<Integer> taskIds;
  private TaskSchedulePlan previousTaskSchedulePlan;
  private Resource instanceDefaultResourceValue;
  private Resource containerMaximumResourceValue;
  private Map<String, Double> TaskRamMap;
  private Map<String, Double> TaskDiskMap;
  private Map<String, Double> TaskCPUMap;
  private int requestedContainerPadding;
  private int numContainers;
  private Map<Integer, Container> containers;
  private int numberOfContainers;

  public TaskSchedulePlanBuilder(int jobId) {
  }

  public TaskSchedulePlanBuilder(int jobId, TaskSchedulePlan previousTaskSchedulePlan) {
    this.jobId = jobId;
    this.previousTaskSchedulePlan = previousTaskSchedulePlan;
    this.numContainers = 0;
    this.requestedContainerPadding = 0;
    this.TaskRamMap = new HashMap<> ();
    this.TaskDiskMap = new HashMap<> ();
    this.TaskCPUMap = new HashMap<> ();
  }

  public Map<Integer, Container> getContainers() {
    return containers;
  }

  public void setContainers(Map<Integer, Container> containers) {
    this.containers = containers;
  }
  public Map<String, Double> getTaskRamMap() {
    return TaskRamMap;
  }

  public TaskSchedulePlanBuilder setTaskRamMap(Map<String, Double> taskRamMap) {
    this.TaskRamMap = taskRamMap;
    return this;
  }
  public Map<String, Double> getTaskDiskMap() {
    return TaskDiskMap;
  }

  public TaskSchedulePlanBuilder setTaskDiskMap(Map<String, Double> taskDiskMap) {
    this.TaskDiskMap = taskDiskMap;
    return this;
  }

  public Map<String, Double> getTaskCPUMap() {
    return TaskCPUMap;
  }

  public TaskSchedulePlanBuilder setTaskCPUMap(Map<String, Double> taskCPUMap) {
    TaskCPUMap = taskCPUMap;
    return this;
  }

  public int getJobId() {
    return jobId;
  }

  public void setJobId(int jobId) {
    this.jobId = jobId;
  }

  public int getNumNodes() {
    return numNodes;
  }

  public void setNumNodes(int numNodes) {
    this.numNodes = numNodes;
  }

  public ArrayList<Integer> getTaskIds() {
    return taskIds;
  }

  public void setTaskIds(ArrayList<Integer> taskIds) {
    this.taskIds = taskIds;
  }

  public TaskSchedulePlan getPreviousTaskSchedulePlan() {
    return previousTaskSchedulePlan;
  }

  public TaskSchedulePlanBuilder setPreviousTaskSchedulePlan(TaskSchedulePlan previousTaskSchedulePlan) {
    this.previousTaskSchedulePlan = previousTaskSchedulePlan;
    return this;
  }

  public Resource getInstanceDefaultResourceValue() {
    return instanceDefaultResourceValue;
  }

  public TaskSchedulePlanBuilder setInstanceDefaultResourceValue(Resource instanceDefaultResourceValue) {
    this.instanceDefaultResourceValue = instanceDefaultResourceValue;
    return this;
  }

  public Resource getContainerMaximumResourceValue() {
    return containerMaximumResourceValue;
  }

  public TaskSchedulePlanBuilder setContainerMaximumResourceValue(Resource containerMaximumResourceValue) {
    this.containerMaximumResourceValue = containerMaximumResourceValue;
    return this;
  }



  public int getRequestedContainerPadding() {
    return requestedContainerPadding;
  }

  public TaskSchedulePlanBuilder setRequestedContainerPadding(int requestedContainerPadding) {
    this.requestedContainerPadding = requestedContainerPadding;
    return this;
  }

  public int getNumContainers() {
    return numContainers;
  }

  public void setNumContainers(int numContainers) {
    this.numContainers = numContainers;
  }

  public TaskSchedulePlan build() {

    Set<TaskSchedulePlan.ContainerPlan> containerPlans = buildContainerPlans(
        this.containers, this.TaskRamMap, this.TaskDiskMap, this.TaskCPUMap,
        this.instanceDefaultResourceValue, this.requestedContainerPadding);

    return new TaskSchedulePlan (jobId, containerPlans);
  }

  private Set<TaskSchedulePlan.ContainerPlan> buildContainerPlans(
      Map<Integer, Container> containers, Map<String, Double> taskRamMap, Map<String, Double> taskDiskMap,
      Map<String, Double> taskCPUMap, Resource instanceDefaultResourceValue, int requestedContainerPadding) {

    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new LinkedHashSet<>();

    for (Integer containerId : containers.keySet()) {
      Container container = containers.get(containerId);
      if (container.getTaskInstances ().size() == 0) {
        continue;
      }
      Double containerRAMValue = 0.0;
      Double containerDiskValue = 0.0;
      Double containerCPUValue = 0.0;

      // Calculate the resource required for single instance
      Set<TaskSchedulePlan.TaskInstancePlan> taskInstancePlans = new HashSet<>();

      for (TaskSchedulePlan.TaskInstancePlan taskInstancePlan : container.getTaskInstances ()) {

        InstanceId instanceId = new InstanceId(taskInstancePlan.getTaskName(),
            taskInstancePlan.getTaskId(), taskInstancePlan.getTaskIndex());

        Double instanceRAMValue;
        Double instanceDiskValue;
        Double instanceCPUValue;

        if (taskRamMap.containsKey(instanceId.getTaskName())) {
          instanceRAMValue = taskRamMap.get(instanceId.getTaskName());
        } else {
          instanceRAMValue = instanceDefaultResourceValue.getRam();
        }
        //containerRam = containerRam.plus(instanceRAMValue);
        containerRAMValue += instanceRAMValue;

        if (taskDiskMap.containsKey (instanceId.getTaskName ())) {
          instanceDiskValue = instanceDefaultResourceValue.getDisk ();
        } else {
          instanceDiskValue = instanceDefaultResourceValue.getDisk ();
        }
        containerDiskValue += instanceDiskValue;

        if (taskCPUMap.containsKey (instanceId.getTaskName ())) {
          instanceCPUValue = instanceDefaultResourceValue.getCpu ();
        } else {
          instanceCPUValue = instanceDefaultResourceValue.getCpu ();
        }
        containerCPUValue += instanceCPUValue;

        Resource resource  = new Resource (instanceRAMValue, instanceDiskValue, instanceCPUValue);
        taskInstancePlans.add (new TaskSchedulePlan.TaskInstancePlan (instanceId.getTaskName (), instanceId.getTaskId (), instanceId.getTaskIndex (), resource));
        //taskInstancePlanMap.put(id, new TaskSchedulePlan.TaskInstancePlan("mpitask", 1, 1, resource));
      }

      //It should be edited with proper values;;;

      /*containerCpu += (paddingPercentage * containerCpu) / 100;
      containerRam = containerRam.increaseBy(paddingPercentage);
      containerDiskInBytes = containerDiskInBytes.increaseBy(paddingPercentage);*/

      Resource resource = new Resource(containerRAMValue, containerDiskValue, containerCPUValue);
      TaskSchedulePlan.ContainerPlan containerPlan =
          new TaskSchedulePlan.ContainerPlan(containerId, taskInstancePlans, resource);
      containerPlans.add(containerPlan);
    }
    return containerPlans;
  }

  public TaskSchedulePlanBuilder updateNumContainers(int numberOfContainers) {
    this.numContainers = numberOfContainers;
    return this;
  }

  /*private int jobId;
  private int numNodes;
  private ArrayList<Integer> taskIds;
  private TaskSchedulePlan previousTaskSchedulePlan;
  private Resource instanceDefaultResourceValue;
  private Resource containerMaximumResourceValue;
  private Map<String, Double> componentRamMap;
  private int requestedContainerPadding;
  private int numContainers;

  public TaskSchedulePlanBuilder(int jobId) {
    this.jobId = jobId;
    this.previousTaskSchedulePlan = null;
  }

  public TaskSchedulePlanBuilder(int jobId, TaskSchedulePlan previousTaskSchedulePlan) {
    this.jobId = jobId;
    this.previousTaskSchedulePlan = previousTaskSchedulePlan;
    this.numContainers = 0;
    this.requestedContainerPadding = 0;
    this.componentRamMap = new HashMap<> ();
  }

  public void newTaskSchedulePlanBuilder(){

  }

  public int getJobId() {
    return jobId;
  }

  public void setJobId(int jobId) {
    this.jobId = jobId;
  }

  public int getNumNodes() {
    return numNodes;
  }

  public void setNumNodes(int numNodes) {
    this.numNodes = numNodes;
  }

  public ArrayList<Integer> getTaskIds() {
    return taskIds;
  }

  public void setTaskIds(ArrayList<Integer> taskIds) {
    this.taskIds = taskIds;
  }

  public TaskSchedulePlan getPreviousTaskSchedulePlan() {
    return previousTaskSchedulePlan;
  }

  public TaskSchedulePlan setPreviousTaskSchedulePlan(TaskSchedulePlan previousTaskSchedulePlan) {
    this.previousTaskSchedulePlan = previousTaskSchedulePlan;
    return this.previousTaskSchedulePlan;
  }

  public Resource getInstanceDefaultResourceValue() {
    return instanceDefaultResourceValue;
  }

  public TaskSchedulePlan setInstanceDefaultResourceValue(Resource instanceDefaultResourceValue) {
    this.instanceDefaultResourceValue = instanceDefaultResourceValue;
    return this;

  }

  public Resource getContainerMaximumResourceValue() {
    return containerMaximumResourceValue;
  }

  public void setContainerMaximumResourceValue(Resource containerMaximumResourceValue) {
    this.containerMaximumResourceValue = containerMaximumResourceValue;
  }

  public Map<String, Double> getComponentRamMap() {
    return componentRamMap;
  }

  public void setComponentRamMap(Map<String, Double> componentRamMap) {
    this.componentRamMap = componentRamMap;
  }

  public int getRequestedContainerPadding() {
    return requestedContainerPadding;
  }

  public void setRequestedContainerPadding(int requestedContainerPadding) {
    this.requestedContainerPadding = requestedContainerPadding;
  }

  public int getNumContainers() {
    return numContainers;
  }

  public void setNumContainers(int numContainers) {
    this.numContainers = numContainers;
  }
  */
}
