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
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class TaskSchedulePlanBuilder {

  private static final Logger LOG = Logger.getLogger(TaskSchedulePlanBuilder.class.getName());

  private TaskSchedulePlan previousTaskSchedulePlan;
  private Resource instanceDefaultResourceValue;
  private Resource containerMaximumResourceValue;
  private Map<String, Double> taskRamMap;
  private Map<String, Double> taskDiskMap;
  private Map<String, Double> taskCpuMap;
  private Map<Integer, Container> containers;
  private TreeSet<Integer> taskIds;
  private HashMap<String, TreeSet<Integer>> taskIndexes;
  private int requestedContainerPadding;
  private int numContainers;
  private int numberOfContainers;
  private int jobId;

  public TaskSchedulePlanBuilder(int jobId) {
    this(jobId, null);
  }

  public TaskSchedulePlanBuilder(int jobId, TaskSchedulePlan previousTaskSchedulePlan) {
    this.jobId = jobId;
    this.previousTaskSchedulePlan = previousTaskSchedulePlan;
    this.numContainers = 0;
    this.requestedContainerPadding = 0;
    this.taskRamMap = new HashMap<>();
    this.taskDiskMap = new HashMap<>();
    this.taskCpuMap = new HashMap<>();
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

  @VisibleForTesting
  static List<Container> sortContainers(List<Scorer<Container>> scorers,
                                        Collection<Container> containers) {
    List<Container> sorted = new ArrayList<>(containers);
    Collections.sort(sorted, new ChainedContainerComparator<>(scorers));
    return sorted;
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
    return taskRamMap;
  }

  public TaskSchedulePlanBuilder setTaskRamMap(Map<String, Double> taskramMap) {
    this.taskRamMap = taskramMap;
    return this;
  }

  public Map<String, Double> getTaskDiskMap() {
    return taskDiskMap;
  }

  public TaskSchedulePlanBuilder setTaskDiskMap(Map<String, Double> taskdiskMap) {
    this.taskDiskMap = taskdiskMap;
    return this;
  }

  public Map<String, Double> getTaskCpuMap() {
    return taskCpuMap;
  }

  public TaskSchedulePlanBuilder setTaskCpuMap(Map<String, Double> taskcpuMap) {
    this.taskCpuMap = taskcpuMap;
    return this;
  }

  public int getJobId() {
    return jobId;
  }

  public void setJobId(int jobId) {
    this.jobId = jobId;
  }

  public TaskSchedulePlan getPreviousTaskSchedulePlan() {
    return previousTaskSchedulePlan;
  }

  public TaskSchedulePlanBuilder setPreviousTaskSchedulePlan(TaskSchedulePlan
                                                                 previousTaskschedulePlan) {
    this.previousTaskSchedulePlan = previousTaskschedulePlan;
    return this;
  }

  public Resource getInstanceDefaultResourceValue() {
    return instanceDefaultResourceValue;
  }

  public TaskSchedulePlanBuilder setInstanceDefaultResourceValue(Resource
                                                                     instancedefaultResourcevalue) {
    this.instanceDefaultResourceValue = instancedefaultResourcevalue;
    return this;
  }

  public Resource getContainerMaximumResourceValue() {
    return containerMaximumResourceValue;
  }

  public TaskSchedulePlanBuilder setContainerMaximumResourceValue(
      Resource containerMaxResourceValue) {
    this.containerMaximumResourceValue = containerMaxResourceValue;
    return this;
  }

  public int getRequestedContainerPadding() {
    return requestedContainerPadding;
  }

  public TaskSchedulePlanBuilder setRequestedContainerPadding(
      int reqContainerPadding) {
    this.requestedContainerPadding = reqContainerPadding;
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
        this.containers, this.taskRamMap, this.taskDiskMap, this.taskCpuMap,
        this.instanceDefaultResourceValue, this.requestedContainerPadding);
    return new TaskSchedulePlan(jobId, containerPlans);
  }

  private Set<TaskSchedulePlan.ContainerPlan> buildContainerPlans(Map<Integer,
      Container> containerValue,
                                                                  Map<String, Double> taskramMap,
                                                                  Map<String, Double> taskdiskmap,
                                                                  Map<String, Double> taskcpumap,
                                                                  Resource instdefaultresourceValue,
                                                                  int containerPadding) {

    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new LinkedHashSet<>();
    try {
      for (Integer containerId : containerValue.keySet()) {
        Container container = containerValue.get(containerId);
        if (container.getTaskInstances().size() == 0) {
          continue;
        }
        Double containerRAMValue = 0.0;
        Double containerDiskValue = 0.0;
        Double containerCPUValue = 0.0;

        Set<TaskSchedulePlan.TaskInstancePlan> taskInstancePlans = new HashSet<>();

        LOG.info("Total Scheduled Task Instances Size:" + container.getTaskInstances().size());

        for (TaskSchedulePlan.TaskInstancePlan taskInstancePlan : container.getTaskInstances()) {

          TaskInstanceId instanceId = new TaskInstanceId(taskInstancePlan.getTaskName(),
              taskInstancePlan.getTaskId(), taskInstancePlan.getTaskIndex());

          Double instanceRAMValue;
          if (taskramMap.containsKey(instanceId.getTaskName())) {
            instanceRAMValue = taskramMap.get(instanceId.getTaskName());
          } else {
            instanceRAMValue = instdefaultresourceValue.getRam();
          }
          containerRAMValue += instanceRAMValue;

          /*instanceDiskValue = instdefaultresourceValue.getDisk();
          containerDiskValue += instanceDiskValue;

          instanceCPUValue = instdefaultresourceValue.getCpu();
          containerCPUValue += instanceCPUValue;*/

          Double instanceDiskValue;
          if (taskDiskMap.containsKey(instanceId.getTaskName())) {
            instanceDiskValue = taskDiskMap.get(instanceId.getTaskName());
          } else {
            instanceDiskValue = instdefaultresourceValue.getDisk();
          }
          containerDiskValue += instanceDiskValue;

          Double instanceCPUValue;
          if (taskCpuMap.containsKey(instanceId.getTaskName())) {
            instanceCPUValue = taskCpuMap.get(instanceId.getTaskName());
          } else {
            instanceCPUValue = instdefaultresourceValue.getCpu();
          }
          containerCPUValue += instanceCPUValue;

          Resource resource = new Resource(instanceRAMValue, instanceDiskValue, instanceCPUValue);

          taskInstancePlans.add(new TaskSchedulePlan.TaskInstancePlan(instanceId.getTaskName(),
              instanceId.getTaskId(), instanceId.getTaskIndex(), resource));
        }

        containerCPUValue += (requestedContainerPadding * containerCPUValue) / 100;
        containerRAMValue += containerRAMValue + requestedContainerPadding;
        containerDiskValue += containerDiskValue + requestedContainerPadding;

        //containerRAMValue = containerRAMValue.increaseBy(requestedContainerPadding);
        //containerDiskValue = containerDiskValue.increaseBy(requestedContainerPadding);

        LOG.info("ResourceContainer CPU Value:" + containerCPUValue);

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

  public TaskSchedulePlanBuilder updateNumContainers(int numOfContainers) {
    this.numContainers = numOfContainers;
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

  public TaskSchedulePlanBuilder addInstance(Integer containerId, String taskName)
      throws NullPointerException {

    initContainer(containerId.intValue());

    Integer taskId = taskIds.isEmpty() ? 1 : taskIds.last() + 1;
    Integer taskIndex = taskIndexes.get(taskName) != null
        ? taskIndexes.get(taskName).last() + 1 : 0;

    TaskInstanceId taskInstanceId = new TaskInstanceId(taskName, taskId, taskIndex);

    //This value should be modified with the appropriate values...!
    double instanceRam = this.instanceDefaultResourceValue.getRam();
    double instanceDisk = this.instanceDefaultResourceValue.getDisk();
    double instanceCPU = this.instanceDefaultResourceValue.getCpu();

    if (this.getTaskRamMap().containsKey(taskName)
        && this.getTaskDiskMap().containsKey(taskName)
        && this.getTaskCpuMap().containsKey(taskName)) {
      instanceRam = getTaskRamMap().get(taskName);
      instanceDisk = getTaskDiskMap().get(taskName);
      instanceCPU = getTaskCpuMap().get(taskName);
    }
    LOG.info("Instance Required Values For the Task:"
        + "\t" + taskIndex + "\t" + taskName + "\t" + instanceRam
        + "\t" + instanceDisk + "\t" + instanceCPU);

    Resource resource = new Resource(instanceRam, instanceDisk, instanceCPU);
    addToContainer(containers.get(containerId),
        new TaskSchedulePlan.TaskInstancePlan(taskInstanceId, resource),
        this.taskIndexes, this.taskIds);


    return this;
  }

  private void initContainers() {

    Map<Integer, Container> containerMap = this.containers;
    HashMap<String, TreeSet<Integer>> taskindexes = this.taskIndexes;
    TreeSet<Integer> taskids = this.taskIds;

    if (taskindexes == null) {
      taskindexes = new HashMap<>();
    }

    if (taskids == null) {
      taskids = new TreeSet<>();
    }

    if (containerMap == null) {
      if (previousTaskSchedulePlan == null) {
        containerMap = new HashMap<>();
        for (int containerId = 1; containerId <= numContainers; containerId++) {
          containerMap.put(containerId, new Container(containerId,
              this.containerMaximumResourceValue, this.requestedContainerPadding));
        }
      }
    } else {
      try {
        if (this.previousTaskSchedulePlan != null) {
          containerMap = getContainers(this.previousTaskSchedulePlan,
              this.requestedContainerPadding, taskindexes, taskids);
        }
      } catch (NullPointerException e) {
        e.printStackTrace();
      }
    }
    this.taskIds = taskids;
    this.taskIndexes = taskindexes;
    this.containers = containerMap;
    LOG.info("ResourceContainer size value is:" + containers.size()
        + "\t" + this.taskIds + "\t" + this.taskIndexes);
  }

  private void initContainer(int containerId) {
    initContainers();
    if (this.containers.get(containerId) == null) {
      this.containers.put(containerId, new Container(
          containerId, this.containerMaximumResourceValue, this.requestedContainerPadding));
    }
  }

  private Map<Integer, Container> getContainers(TaskSchedulePlan previoustaskscheduleplan,
                                                int requestedcontainerpadding, HashMap<String,
      TreeSet<Integer>> taskindexes,
                                                TreeSet<Integer> taskids) {
    if (previoustaskscheduleplan != null) {
      LOG.info("Previous Task Schedule Plan is:" + previoustaskscheduleplan);
    }
    Map<Integer, Container> containerMap = new HashMap<>();
    Resource resource = null;
    try {
      resource = previoustaskscheduleplan.getMaxContainerResources();
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    try {
      LOG.info("ResourceContainer inside the container:" + resource);
    } catch (Exception ee) {
      ee.printStackTrace();
    }

    for (TaskSchedulePlan.ContainerPlan currentContainerPlan
        : previoustaskscheduleplan.getContainers()) {
      Container container = new Container(currentContainerPlan.getContainerId(),
          resource, requestedcontainerpadding);
      for (TaskSchedulePlan.TaskInstancePlan taskInstancePlan
          : currentContainerPlan.getTaskInstances()) {
        addToContainer(container, taskInstancePlan, taskindexes, taskids);
      }
      containerMap.put(currentContainerPlan.getContainerId(), container);
    }

    if (!containerMap.isEmpty()) {
      LOG.info("ResourceContainer values are:" + containerMap);
    } else {
      LOG.info("container values are empty");
    }
    return containerMap;
  }

  /***********For testing use the code from heron ***********************/
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
      LOG.info("ResourceContainer Id and task Name:" + containerId + "\t" + taskName);
    }
    return containerId;
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
}
