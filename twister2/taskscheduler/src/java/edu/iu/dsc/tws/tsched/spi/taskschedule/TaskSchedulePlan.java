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
package edu.iu.dsc.tws.tsched.spi.taskschedule;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

/**
 * This class is responsible for constructing the container plan, instance plan, and task schedule plan along
 * with their resource requirements.
 */

public class TaskSchedulePlan {

  private final Set<ContainerPlan> containers;
  private final Map<Integer, ContainerPlan> containersMap;
  private int jobId;

  public TaskSchedulePlan(int id, Set<ContainerPlan> containers) {
    this.jobId = id;
    this.containers = ImmutableSet.copyOf(containers);
    containersMap = new HashMap<>();
    for (ContainerPlan containerPlan : containers) {
      containersMap.put(containerPlan.containerId, containerPlan);
    }
  }

  public int getJobId() {
    return jobId;
  }

  public void setJobId(int id) {
    this.jobId = id;
  }

  public Map<Integer, ContainerPlan> getContainersMap() {
    return containersMap;
  }

  public Set<ContainerPlan> getContainers() {
    return containers;
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

    if (jobId != that.jobId) {
      return false;
    }
    return containers.equals(that.containers) && containersMap.equals(that.containersMap);
  }

  @Override
  public int hashCode() {
    int result = jobId;
    result = 31 * result + containers.hashCode();
    result = 31 * result + containersMap.hashCode();
    return result;
  }

  public static class TaskInstancePlan {

    private final String taskName;
    private final int taskId;
    private final int taskIndex;
    private final Resource resource;

    public TaskInstancePlan(String taskName, int taskId, int taskIndex, Resource resource) {
      this.taskName = taskName;
      this.taskId = taskId;
      this.taskIndex = taskIndex;
      this.resource = resource;
    }

    public TaskInstancePlan(TaskInstanceId taskInstanceId, Resource resource) {
      this.taskName = taskInstanceId.getTaskName();
      this.taskId = taskInstanceId.getTaskId();
      this.taskIndex = taskInstanceId.getTaskIndex();
      this.resource = resource;
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

    public Resource getResource() {
      return resource;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TaskInstancePlan)) {
        return false;
      }

      TaskInstancePlan that = (TaskInstancePlan) o;

      if (taskId != that.taskId) {
        return false;
      }
      if (taskIndex != that.taskIndex) {
        return false;
      }
      if (taskName != null ? !taskName.equals(that.taskName) : that.taskName != null) {
        return false;
      }
      return resource != null ? resource.equals(that.resource) : that.resource == null;
    }

    @Override
    public int hashCode() {
      int result = taskName != null ? taskName.hashCode() : 0;
      result = 31 * result + taskId;
      result = 31 * result + taskIndex;
      result = 31 * result + (resource != null ? resource.hashCode() : 0);
      return result;
    }
  }

  public static class ContainerPlan {

    private final int containerId;
    private final Set<TaskInstancePlan> taskInstances;
    private final Resource requiredResource;
    private final Optional<Resource> scheduledResource;

    public ContainerPlan(int id, Set<TaskInstancePlan> instances, Resource requiredResource) {
      this(id, instances, requiredResource, null);
    }

    public ContainerPlan(int id,
                         Set<TaskInstancePlan> taskInstances,
                         Resource requiredResource,
                         Resource scheduledResource) {
      this.containerId = id;
      this.taskInstances = ImmutableSet.copyOf(taskInstances);
      this.requiredResource = requiredResource;
      this.scheduledResource = Optional.fromNullable(scheduledResource);
    }

    public int getContainerId() {
      return containerId;
    }

    public Set<TaskInstancePlan> getTaskInstances() {
      return taskInstances;
    }

    public Resource getRequiredResource() {
      return requiredResource;
    }

    public Optional<Resource> getScheduledResource() {
      return scheduledResource;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ContainerPlan)) {
        return false;
      }

      ContainerPlan that = (ContainerPlan) o;

      if (containerId != that.containerId) {
        return false;
      }
      if (!taskInstances.equals(that.taskInstances)) {
        return false;
      }
      if (!requiredResource.equals(that.requiredResource)) {
        return false;
      }
      return scheduledResource.equals(that.scheduledResource);
    }

    @Override
    public int hashCode() {
      int result = containerId;
      result = 31 * result + taskInstances.hashCode();
      result = 31 * result + requiredResource.hashCode();
      result = 31 * result + scheduledResource.hashCode();
      return result;
    }
  }
}

