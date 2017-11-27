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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;

import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedule;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceId;

public class TaskSchedulePlanBuilder {

  private TaskSchedulePlan previousTaskSchedulePlan;
  private Resource instanceDefaultResourceValue;
  private Resource containerMaximumResourceValue;

  private Map<String, Double> TaskRamMap;
  private Map<String, Double> TaskDiskMap;
  private Map<String, Double> TaskCPUMap;
  private Map<Integer, Container> containers;
  private TreeSet<Integer> taskIds;
  private HashMap<String, TreeSet<Integer>> taskIndexes;

  private int requestedContainerPadding;
  private int numContainers;
  //private int numNodes;
  private int numberOfContainers;
  private int jobId;

  public TaskSchedulePlanBuilder(int jobId) {
    //this(jobId, previousTaskSchedulePlan:null);

  }

  public TaskSchedulePlanBuilder(int jobId, TaskSchedulePlan previousTaskSchedulePlan) {

    this.jobId = jobId;
    this.previousTaskSchedulePlan = previousTaskSchedulePlan;
    this.numContainers = 0;
    this.requestedContainerPadding = 0;
    this.TaskRamMap = new HashMap<>();
    this.TaskDiskMap = new HashMap<>();
    this.TaskCPUMap = new HashMap<>();

  }

  public void setTaskIds(TreeSet<Integer> taskIds) {
    this.taskIds = taskIds;
  }

  public int getNumberOfContainers() {
    return numberOfContainers;
  }

  public void setNumberOfContainers(int numberOfContainers) {
    this.numberOfContainers = numberOfContainers;
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

    /*public int getNumNodes() {
        return numNodes;
    }

    public void setNumNodes(int numNodes) {
        this.numNodes = numNodes;
    }*/

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
    return new TaskSchedulePlan(jobId, containerPlans);
  }

  private Set<TaskSchedulePlan.ContainerPlan> buildContainerPlans(
      Map<Integer, Container> containers, Map<String, Double> taskRamMap, Map<String, Double> taskDiskMap,
      Map<String, Double> taskCPUMap, Resource instanceDefaultResourceValue, int requestedContainerPadding) {

    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new LinkedHashSet<>();
    try {
      for (Integer containerId : containers.keySet()) {
        Container container = containers.get(containerId);
        if (container.getTaskInstances().size() == 0) {
          continue;
        }
        Double containerRAMValue = 0.0;
        Double containerDiskValue = 0.0;
        Double containerCPUValue = 0.0;

        Set<TaskSchedulePlan.TaskInstancePlan> taskInstancePlans = new HashSet<>();

        for (TaskSchedulePlan.TaskInstancePlan taskInstancePlan : container.getTaskInstances()) {

          TaskInstanceId instanceId = new TaskInstanceId(taskInstancePlan.getTaskName(),
              taskInstancePlan.getTaskId(), taskInstancePlan.getTaskIndex());

          Double instanceRAMValue;
          Double instanceDiskValue;
          Double instanceCPUValue;

          if (taskRamMap.containsKey(instanceId.getTaskName())) {
            instanceRAMValue = taskRamMap.get(instanceId.getTaskName());
          } else {
            instanceRAMValue = instanceDefaultResourceValue.getRam();
          }
          containerRAMValue += instanceRAMValue;

          System.out.println("ResourceContainer Ram Value:" + containerRAMValue);

          if (taskDiskMap.containsKey(instanceId.getTaskName())) {
            instanceDiskValue = instanceDefaultResourceValue.getDisk();
          } else {
            instanceDiskValue = instanceDefaultResourceValue.getDisk();
          }
          containerDiskValue += instanceDiskValue;

          System.out.println("ResourceContainer Disk Value:" + containerDiskValue);

          if (taskCPUMap.containsKey(instanceId.getTaskName())) {
            instanceCPUValue = instanceDefaultResourceValue.getCpu();
          } else {
            instanceCPUValue = instanceDefaultResourceValue.getCpu();
          }
          containerCPUValue += instanceCPUValue;

          Resource resource = new Resource(instanceRAMValue, instanceDiskValue, instanceCPUValue);

          taskInstancePlans.add(new TaskSchedulePlan.TaskInstancePlan
              (instanceId.getTaskName(), instanceId.getTaskId(), instanceId.getTaskIndex(), resource));

          //taskInstancePlans.add(new TaskSchedulePlan.TaskInstancePlan (instanceId,
          //      new ResourceContainer(instanceRAMValue, instanceDiskValue, instanceCPUValue)));
        }

        System.out.println("ResourceContainer CPU Value:" + containerCPUValue);
        /**** containerCpu += (paddingPercentage * containerCpu) / 100;
         containerRam = containerRam.increaseBy(paddingPercentage);
         containerDiskInBytes = containerDiskInBytes.increaseBy(paddingPercentage); ****/

        Resource resource = new Resource(containerRAMValue, containerDiskValue, containerCPUValue);
        TaskSchedulePlan.ContainerPlan containerPlan =
            new TaskSchedulePlan.ContainerPlan(containerId, taskInstancePlans, resource);
        containerPlans.add(containerPlan);
      }
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return containerPlans;
  }

  public TaskSchedulePlanBuilder updateNumContainers(int numberOfContainers) {
    this.numContainers = numberOfContainers;
    return this;
  }

  public TaskSchedulePlanBuilder addInstance(Integer containerId, String taskName) throws NullPointerException {

    Integer taskId = taskIds.isEmpty() ? 1 : taskIds.last() + 1;
    Integer taskIndex = taskIndexes.get(taskName) != null
        ? taskIndexes.get(taskName).last() + 1 : 0;

    TaskInstanceId taskInstanceId = new TaskInstanceId(taskName, taskId, taskIndex);

    //This value should be modified with an appropriate values...!
    double instanceRam = this.instanceDefaultResourceValue.getRam();
    double instanceDisk = this.instanceDefaultResourceValue.getDisk();
    double instanceCPU = this.instanceDefaultResourceValue.getCpu();

    if (this.getTaskRamMap().containsKey(taskName)
        && this.getTaskDiskMap().containsKey(taskName)
        && this.getTaskCPUMap().containsKey(taskName)) {
      instanceRam = getTaskRamMap().get(taskName);
      instanceDisk = getTaskDiskMap().get(taskName);
      instanceCPU = getTaskCPUMap().get(taskName);
    }

    System.out.println("Instance Ram value for the task in line number 273 @Taskscheduleplanbuilder:" + instanceRam + "\t" + taskName + "\t" + instanceDisk + "\t" + instanceCPU);

    double instanceRAMValue = 4.0;
    double instanceDiskValue = 10.0;
    double instanceCPUValue = 5.0;

    System.out.println("Instance Ram Value:" + instanceRAMValue + "\t" + "instance disk value:" + instanceDiskValue + "instance cpu value:" + instanceCPUValue);

    Resource resource = new Resource(instanceRAMValue, instanceDiskValue, instanceCPUValue);
    addToContainer(containers.get(containerId),
        new TaskSchedulePlan.TaskInstancePlan(taskInstanceId, resource), this.taskIndexes, this.taskIds);
    return this;
  }

  public int addInstance(String taskName) {

    initContainers();
    int containerId = 1;
    for (Container container : this.containers.values()) {
      addInstance(container.getContainerId(), taskName);
      containerId = container.getContainerId();
    }
    return containerId;
  }

  private void initContainers() {

    Map<Integer, Container> containerMap = this.containers;
    HashMap<String, TreeSet<Integer>> newtaskIndexes = this.taskIndexes;
    TreeSet<Integer> newtaskIds = this.taskIds;

    if (newtaskIndexes == null) {
      newtaskIndexes = new HashMap<>();
    }

    if (newtaskIds == null) {
      newtaskIds = new TreeSet<>();
    }

    if (containerMap == null) {
      if (previousTaskSchedulePlan == null) {
        containerMap = new HashMap<>();
        for (int containerId = 1; containerId <= numContainers; containerId++) {
          containerMap.put(containerId, new Container(containerId, this.containerMaximumResourceValue, this.requestedContainerPadding));
        }
      }
    } else {
      try {

        if (this.previousTaskSchedulePlan != null) {
          containerMap = getContainers(this.previousTaskSchedulePlan, this.requestedContainerPadding,
              newtaskIndexes, newtaskIds);
        }
      } catch (NullPointerException e) {
        e.printStackTrace();
      }

    }

    this.taskIds = newtaskIds;
    this.taskIndexes = newtaskIndexes;
    this.containers = containerMap;
    System.out.println("ResourceContainer size value is:" + containers.size() + "\t" + this.taskIds + "\t" + this.taskIndexes);
  }

  private Map<Integer, Container> getContainers(TaskSchedulePlan previousTaskSchedulePlan, int requestedContainerPadding, HashMap<String, TreeSet<Integer>> taskIndexes, TreeSet<Integer> taskIds) {


    if (previousTaskSchedulePlan != null) {
      System.out.println("Previous Task Schedule Plan is:" + previousTaskSchedulePlan);
    }
    Map<Integer, Container> containers = new HashMap<>();

        /*ResourceContainer resource = null;
        try {
          ResourceContainer resource = previousTaskSchedulePlan.getMaxContainerResources ();
        } catch (NullPointerException ne){
            ne.printStackTrace ();
        }*/

    //for testing
    Resource resource = new Resource(2.0, 5.0, 5.0);

    System.out.println("ResourceContainer inside the container:" + resource);

    for (TaskSchedulePlan.ContainerPlan currentContainerPlan : previousTaskSchedulePlan.getContainers()) {
      Container container = new Container
          (currentContainerPlan.getContainerId(), resource, requestedContainerPadding);
      for (TaskSchedulePlan.TaskInstancePlan taskInstancePlan : currentContainerPlan.getTaskInstances()) {
        addToContainer(container, taskInstancePlan, taskIndexes, taskIds);
      }
      containers.put(currentContainerPlan.getContainerId(), container);
    }

    if (!containers.isEmpty()) {
      System.out.println("ResourceContainer values are:" + containers);
    } else {
      System.out.println("container values are empty");
    }
    return containers;
  }

  /**
   * Add instancePlan to container and update the componentIndexes and taskIds indexes
   */
  private static void addToContainer(Container container,
                                     TaskSchedulePlan.TaskInstancePlan taskInstancePlan,
                                     Map<String, TreeSet<Integer>> taskIndexes,
                                     Set<Integer> taskIds) {
    container.add(taskInstancePlan);

    String taskName = taskInstancePlan.getTaskName();
    if (taskIndexes.get(taskName) == null) {
      taskIndexes.put(taskName, new TreeSet<Integer>());
    }
    taskIndexes.get(taskName).add(taskInstancePlan.getTaskIndex());
    taskIds.add(taskInstancePlan.getTaskId());
  }

  /****************************************For testing use the code from heron ***********************/

  public int addInstance(Scorer<Container> scorer,
                         String taskName) {
    List<Scorer<Container>> scorers = new LinkedList<>();
    scorers.add(scorer);
    return addInstance(scorers, taskName);
  }

  private int addInstance(List<Scorer<Container>> scorers, String taskName) {

    initContainers();
    int containerId = 0;
    for (Container container : sortContainers(scorers, this.containers.values())) {
      addInstance(container.getContainerId(), taskName);
      //return container.getContainerId();
      containerId = container.getContainerId();
      System.out.println("ResourceContainer Id and task Name:" + containerId + "\t" + taskName);
    }
    return containerId;
  }

  @VisibleForTesting
  static List<Container> sortContainers(List<Scorer<Container>> scorers,
                                        Collection<Container> containers) {
    List<Container> sorted = new ArrayList<>(containers);
    Collections.sort(sorted, new ChainedContainerComparator<>(scorers));
    return sorted;
  }

  private static class ChainedContainerComparator<T> implements Comparator<T> {
    private final Comparator<T> comparator;
    private final ChainedContainerComparator<T> tieBreaker;

    ChainedContainerComparator(List<Scorer<T>> scorers) {
      this((Queue<Scorer<T>>) new LinkedList<Scorer<T>>(scorers));
    }

    ChainedContainerComparator(Queue<Scorer<T>> scorers) {
      if (scorers.isEmpty()) {
        this.comparator = new EqualsComparator<T>();
        this.tieBreaker = null;
      } else {
        this.comparator = new ContainerComparator<T>(scorers.remove());
        this.tieBreaker = new ChainedContainerComparator<T>(scorers);
      }
    }

    @Override
    public int compare(T thisOne, T thatOne) {
      int delta = comparator.compare(thisOne, thatOne);
      if (delta != 0 || this.tieBreaker == null) {
        return delta;
      }
      return tieBreaker.compare(thisOne, thatOne);
    }
  }

  private static class EqualsComparator<T> implements Comparator<T> {
    @Override
    public int compare(T thisOne, T thatOne) {
      return 0;
    }
  }

  private static class ContainerComparator<T> implements Comparator<T> {
    private Scorer<T> scorer;

    ContainerComparator(Scorer<T> scorer) {
      this.scorer = scorer;
    }

    @Override
    public int compare(T thisOne, T thatOne) {
      int sign = 1;
      if (!scorer.sortAscending()) {
        sign = -1;
      }
      return sign * (getScore(thisOne) - getScore(thatOne));
    }

    private int getScore(T container) {
      return (int) (1000 * scorer.getScore(container));
    }
  }
  /****************************************************************************************/

}
