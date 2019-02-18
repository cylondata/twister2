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

import edu.iu.dsc.tws.tsched.spi.scheduler.TaskSchedulerException;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.TaskScheduleUtils;

/**
 * This class is mainly responsible for building the task schedule plan.
 */
public class TaskSchedulePlanBuilder {

  private static final Logger LOG = Logger.getLogger(TaskSchedulePlanBuilder.class.getName());

  private TaskSchedulePlan previousTaskSchedulePlan;

  private Resource instanceDefaultResourceValue;
  private Resource containerMaximumResourceValue;

  private Map<String, Double> taskRamMap;
  private Map<String, Double> taskCpuMap;
  private Map<String, Double> taskDiskMap;

  private Map<Integer, Container> containers;
  private TreeSet<Integer> taskIds;
  private HashMap<String, TreeSet<Integer>> taskIndexes;

  private int requestedContainerPadding;
  private int numberOfContainers;
  private int id;

  public TaskSchedulePlanBuilder(int scheduleId, TaskSchedulePlan previousTaskSchedulePlan) {
    this.id = scheduleId;
    this.previousTaskSchedulePlan = previousTaskSchedulePlan;
    this.numberOfContainers = 0;
    this.requestedContainerPadding = 0;
    this.taskRamMap = new HashMap<>();
    this.taskCpuMap = new HashMap<>();
  }

  public Map<Integer, Container> getContainers() {
    return containers;
  }

  public void setContainers(Map<Integer, Container> containers) {
    this.containers = containers;
  }

  public TaskSchedulePlanBuilder setTaskRamMap(Map<String, Double> taskramMap) {
    this.taskRamMap = taskramMap;
    return this;
  }

  public TaskSchedulePlanBuilder setTaskDiskMap(Map<String, Double> taskdiskMap) {
    this.taskDiskMap = taskdiskMap;
    return this;
  }

  public TaskSchedulePlanBuilder setTaskCpuMap(Map<String, Double> taskcpuMap) {
    this.taskCpuMap = taskcpuMap;
    return this;
  }

  public int getJobId() {
    return id;
  }

  public void setJobId(int jobId) {
    this.id = jobId;
  }

  //This method will be used for the fault tolerance.
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

  public TaskSchedulePlanBuilder setContainerMaximumResourceValue(
      Resource containerMaxResourceValue) {
    this.containerMaximumResourceValue = containerMaxResourceValue;
    return this;
  }

  public TaskSchedulePlanBuilder setRequestedContainerPadding(
      int reqContainerPadding) {
    this.requestedContainerPadding = reqContainerPadding;
    return this;
  }

  public void setNumContainers(int numContainers) {
    this.numberOfContainers = numContainers;
  }

  public TaskSchedulePlanBuilder updateNumContainers(int numOfContainers) {
    this.numberOfContainers = numOfContainers;
    return this;
  }

  /**
   * It add the instance plan to the container and update the task indexes and task ids indexes.
   */
  private static void addToContainer(Container container,
                                     TaskSchedulePlan.TaskInstancePlan taskInstancePlan,
                                     Map<String, TreeSet<Integer>> taskIndexes,
                                     Set<Integer> taskIds) throws TaskSchedulerException {

    container.add(taskInstancePlan);
    String taskName = taskInstancePlan.getTaskName();

    taskIndexes.computeIfAbsent(taskName, k -> new TreeSet<>());
    taskIndexes.get(taskName).add(taskInstancePlan.getTaskIndex());
    taskIds.add(taskInstancePlan.getTaskId());
  }

  public TaskSchedulePlanBuilder addInstance(Integer containerId, String taskName)
      throws TaskSchedulerException {

    initContainer(containerId);
    int taskId = taskIds.isEmpty() ? 1 : taskIds.last() + 1;
    int taskIndex = taskIndexes.get(taskName) != null
        ? taskIndexes.get(taskName).last() + 1 : 0;


    TaskInstanceId taskInstanceId = new TaskInstanceId(taskName, taskId, taskIndex);
    Resource resource = TaskScheduleUtils.getResourceRequirement(
        taskName, this.taskRamMap, this.instanceDefaultResourceValue,
        this.containerMaximumResourceValue, this.requestedContainerPadding);

    try {
      addToContainer(containers.get(containerId),
          new TaskSchedulePlan.TaskInstancePlan(taskInstanceId, resource),
          this.taskIndexes, this.taskIds);
    } catch (TaskSchedulerException e) {
      throw new TaskSchedulerException(String.format(
          "Insufficient container resources to add instance %s with resources %s to container %d.",
          taskInstanceId, resource, containerId), e);
    }

    LOG.fine("Task id, index, name:" + taskId + "\t" + taskIndex + "\t" + taskName
        + "\tadded to Container:" + containers.get(containerId));
    return this;
  }

  /**
   * It will add the task instance to the container based on the container score value.
   */
  public void addInstance(Scorer<Container> scorer, String taskName)
      throws TaskSchedulerException {
    List<Scorer<Container>> scorers = new LinkedList<>();
    scorers.add(scorer);
    addInstance(scorers, taskName);
  }

  /**
   * This method first initialize the container value then add the task instance in the sorted
   * container score values.
   */
  private void addInstance(List<Scorer<Container>> scorers, String taskName)
      throws TaskSchedulerException {
    initContainers();
    for (Container container : sortContainers(scorers, this.containers.values())) {
      try {
        addInstance(container.getContainerId(), taskName);
        return;
      } catch (TaskSchedulerException ignored) {
      }
    }
    throw new TaskSchedulerException(String.format(
        "Insufficient resources to add '%s' instance to any of the %d containers.",
        taskName, this.containers.size()));
  }

  @VisibleForTesting
  private static List<Container> sortContainers(List<Scorer<Container>> scorers,
                                                Collection<Container> containers) {
    List<Container> sorted = new ArrayList<>(containers);
    sorted.sort(new ChainedContainerComparator<>(scorers));
    return sorted;
  }

  /**
   * This method first validates the available resource settings and invoke the build container
   * plans method to build the container based on the task instance ram, disk, and cpu map values.
   */
  public TaskSchedulePlan build() {
    assertResourceSettings();
    Set<TaskSchedulePlan.ContainerPlan> containerPlans = buildContainerPlans(
        this.containers, this.taskRamMap, this.instanceDefaultResourceValue);
    return new TaskSchedulePlan(id, containerPlans);
  }

  /**
   * This method first initialize the container map values, task index values, and task id sets.
   */
  private void initContainers() {
    //assertResourceSettings();
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
      if (this.previousTaskSchedulePlan == null) {
        containerMap = new HashMap<>();
        for (int containerId = 0; containerId < numberOfContainers; containerId++) {
          containerMap.put(containerId, new Container(containerId,
              this.containerMaximumResourceValue, this.requestedContainerPadding));
        }
      } else {
        try {
          containerMap = getContainers(this.previousTaskSchedulePlan
          );
        } catch (TaskSchedulerException e) {
          throw new TaskSchedulerException(
              "Could not initialize containers using existing packing plan", e);
        }
      }
    }
    if (this.numberOfContainers > containerMap.size()) {
      List<Scorer<Container>> scorers = new ArrayList<>();
      scorers.add(new ContainerIdScorer());
      List<Container> sortedContainers = sortContainers(scorers, containerMap.values());

      int nextContainerId = sortedContainers.get(sortedContainers.size() - 1).getContainerId() + 1;
      Resource capacity =
          containerMap.get(sortedContainers.get(0).getContainerId()).getResource();

      for (int i = 0; i < numberOfContainers - containerMap.size(); i++) {
        containerMap.put(nextContainerId,
            new Container(nextContainerId, capacity, this.requestedContainerPadding));
        nextContainerId++;
      }
    }

    this.taskIds = taskids;
    this.taskIndexes = taskindexes;
    this.containers = containerMap;

    LOG.fine("TaskDetails:" + this.taskIds + "\t" + this.taskIndexes + "\t" + this.containers);
  }

  private void initContainer(int containerId) {
    initContainers();
    if (this.containers.get(containerId) == null) {
      this.containers.put(containerId, new Container(
          containerId, this.containerMaximumResourceValue, this.requestedContainerPadding));
    }
  }

  private void assertResourceSettings() {
    if (this.instanceDefaultResourceValue == null) {
      throw new TaskSchedulerException(
          "defaultInstanceResource must be set on PackingPlanBuilder before modifying containers");
    }
    if (this.containerMaximumResourceValue == null) {
      throw new TaskSchedulerException(
          "maxContainerResource must be set on PackingPlanBuilder before modifying containers");
    }
  }

  private Set<TaskSchedulePlan.ContainerPlan> buildContainerPlans(
      Map<Integer, Container> containerValue,
      Map<String, Double> taskramMap,
      Resource instdefaultresourcevalue) {

    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new LinkedHashSet<>();
    try {
      for (Integer containerId : containerValue.keySet()) {

        Container container = containerValue.get(containerId);
        if (container.getTaskInstances().size() == 0) {
          continue;
        }
        double containerRAMValue = 0.0;
        double containerDiskValue = 0.0;
        double containerCPUValue = 0.0;

        Set<TaskSchedulePlan.TaskInstancePlan> taskInstancePlans = new HashSet<>();

        for (TaskSchedulePlan.TaskInstancePlan taskInstancePlan : container.getTaskInstances()) {

          TaskInstanceId instanceId = new TaskInstanceId(taskInstancePlan.getTaskName(),
              taskInstancePlan.getTaskId(), taskInstancePlan.getTaskIndex());

          double instanceRAMValue;
          if (taskramMap.containsKey(instanceId.getTaskName())) {
            instanceRAMValue = taskramMap.get(instanceId.getTaskName());
          } else {
            instanceRAMValue = instdefaultresourcevalue.getRam();
          }
          containerRAMValue += instanceRAMValue;

          double instanceDiskValue = instdefaultresourcevalue.getDisk();
          containerDiskValue += instanceDiskValue;

          double instanceCPUValue = instdefaultresourcevalue.getCpu();
          containerCPUValue += instanceCPUValue;

          LOG.fine("Resource Container Values:" + "Ram Value:" + containerRAMValue + "\t"
              + "Cpu Value:" + containerCPUValue + "\t" + "Disk Value:" + containerDiskValue);

          Resource resource = new Resource(instanceRAMValue, instanceDiskValue, instanceCPUValue);
          taskInstancePlans.add(new TaskSchedulePlan.TaskInstancePlan(instanceId.getTaskName(),
              instanceId.getTaskId(), instanceId.getTaskIndex(), resource));
        }

        containerCPUValue += (requestedContainerPadding * containerCPUValue) / 100;
        containerRAMValue += containerRAMValue + requestedContainerPadding;
        containerDiskValue += containerDiskValue + requestedContainerPadding;

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

  private Map<Integer, Container> getContainers(TaskSchedulePlan previoustaskschedulePlan)
      throws TaskSchedulerException {

    Map<Integer, Container> containerMap = new HashMap<>();
    Resource resource = previoustaskschedulePlan.getMaxContainerResources();
    for (TaskSchedulePlan.ContainerPlan currentContainerPlan
        : previoustaskschedulePlan.getContainers()) {
      Container container = new Container(
          currentContainerPlan.getContainerId(), resource, requestedContainerPadding);
      for (TaskSchedulePlan.TaskInstancePlan instancePlan
          : currentContainerPlan.getTaskInstances()) {
        try {
          addToContainer(container, instancePlan, taskIndexes, taskIds);
        } catch (TaskSchedulerException e) {
          throw new TaskSchedulerException(String.format(
              "Insufficient container resources to add instancePlan %s to container %s",
              instancePlan, container), e);
        }
      }
      containerMap.put(currentContainerPlan.getContainerId(), container);
    }
    LOG.info("Container Map Values Size Is:" + containerMap.entrySet());
    return containerMap;
  }


  private static class ChainedContainerComparator<T> implements Comparator<T> {
    private final Comparator<T> comparator;
    private final ChainedContainerComparator<T> tieBreaker;

    ChainedContainerComparator(List<Scorer<T>> scorers) {
      this((Queue<Scorer<T>>) new LinkedList<>(scorers));
    }

    ChainedContainerComparator(Queue<Scorer<T>> scorers) {
      if (scorers.isEmpty()) {
        this.comparator = new EqualsComparator<>();
        this.tieBreaker = null;
      } else {
        this.comparator = new ContainerComparator<>(scorers.remove());
        this.tieBreaker = new ChainedContainerComparator<>(scorers);
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
