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
package edu.iu.dsc.tws.tsched.spi.taskschedule;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.ImmutableSet;

import edu.iu.dsc.tws.task.api.schedule.ContainerPlan;
import edu.iu.dsc.tws.task.api.schedule.Resource;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;

/**
 * This class is responsible for generating the task schedule plan which consists of container plan,
 * task instance plan along with their resource requirements.
 */
public class TaskSchedulePlan {

  private final Set<ContainerPlan> containers;

  private final Map<Integer, ContainerPlan> containersMap;
  private int jobId;

  public TaskSchedulePlan(int id, Set<ContainerPlan> containers) {
    this.jobId = id;
    this.containers = ImmutableSet.copyOf(containers);
    containersMap = new TreeMap<>();
    for (ContainerPlan containerPlan : containers) {
      containersMap.put(containerPlan.getContainerId(), containerPlan);
    }
  }

  public Resource getMaxContainerResources() {

    double maxCpu = 0.0;
    double maxRam = 0.0;
    double maxDisk = 0.0;

    for (ContainerPlan containerPlan : getContainers()) {
      Resource containerResource =
          containerPlan.getScheduledResource().orElse(containerPlan.getRequiredResource());
      maxCpu = Math.max(maxCpu, containerResource.getCpu());
      maxRam = Math.max(maxRam, containerResource.getRam());
      maxDisk = Math.max(maxDisk, containerResource.getDisk());
    }
    return new Resource(maxRam, maxDisk, maxCpu);
  }

  private int getTaskSchedulePlanId() {
    return jobId;
  }

  public Map<Integer, ContainerPlan> getContainersMap() {
    return containersMap;
  }

  public Set<ContainerPlan> getContainers() {
    return containers;
  }

  public Map<String, Integer> getTaskCounts() {
    Map<String, Integer> taskCounts = new HashMap<>();
    for (ContainerPlan containerPlan : getContainers()) {
      for (TaskInstancePlan instancePlan : containerPlan.getTaskInstances()) {
        Integer count = 0;
        if (taskCounts.containsKey(instancePlan.getTaskName())) {
          count = taskCounts.get(instancePlan.getTaskName());
        }
        taskCounts.put(instancePlan.getTaskName(), ++count);
      }
    }
    return taskCounts;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TaskSchedulePlan)) {
      return false;
    }

    TaskSchedulePlan that = (TaskSchedulePlan) o;

    return (getTaskSchedulePlanId() == that.getTaskSchedulePlanId())
        && getContainers().equals(that.getContainers());
  }

  @Override
  public int hashCode() {
    int result = containers.hashCode();
    result = 31 * result + jobId;
    return result;
  }
}
