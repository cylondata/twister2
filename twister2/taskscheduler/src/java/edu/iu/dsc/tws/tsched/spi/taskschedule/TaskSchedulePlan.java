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

import com.google.common.base.ImmutableSet;
import com.google.common.base.Optional;

/***
 * This class is responsible for constructing the container plan, instance plan, and task schedule plan along
 * with their resource requirements.
 */

public class TaskSchedulePlan {

  private int jobId;
  private final Set<ContainerPlan> containers;
  private final Map<Integer,ContainerPlan> containersMap;

  public TaskSchedulePlan(int id, Set<ContainerPlan> containers) {
    this.jobId = id;
    this.containers = containers;
    containersMap = new HashMap<>();
    for(ContainerPlan containerPlan:containers){
      containersMap.put(containerPlan.containerId,containerPlan);
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
    if (this == o) return true;
    if (!(o instanceof TaskSchedulePlan)) return false;

    TaskSchedulePlan that = (TaskSchedulePlan) o;

    if (jobId != that.jobId) return false;
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

    public TaskInstancePlan(String taskName, int taskId, int taskIndex, Resource resource){
      this.taskName = taskName;
      this.taskId = taskId;
      this.taskIndex = taskIndex;
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
      if (this == o) return true;
      if (!(o instanceof TaskInstancePlan)) return false;

      TaskInstancePlan that = (TaskInstancePlan) o;

      if (taskId != that.taskId) return false;
      if (taskIndex != that.taskIndex) return false;
      if (taskName != null ? !taskName.equals(that.taskName) : that.taskName != null) return false;
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
    private final TaskInstancePlan taskInstancePlan;
    private final Resource requiredresource;
    private final Optional<Resource> scheduledResource;

    public ContainerPlan(int id, Set<InstancePlan> instances, Resource requiredResource) {
      this(id, instances, requiredResource, null);
    }

    public ContainerPlan(int id,
                         Set<TaskInstancePlan> taskinstances,
                         Resource requiredResource,
                         Resource scheduledResource) {
      this.containerId = id;
      //this.taskInstancePlan = taskinstances;
      this.taskInstancePlan = ImmutableSet.copyOf(instances);
      this.requiredResource = requiredResource;
      this.scheduledResource = Optional.fromNullable(scheduledResource);
    }

    public int getContainerId() {
      return containerId;
    }

    public edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan.TaskInstancePlan getTaskInstancePlan() {
      return taskInstancePlan;
    }

    public Resource getRequiredresource() {
      return requiredresource;
    }

    public Optional<Resource> getScheduledResource() {
      return scheduledResource;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ContainerPlan)) return false;

      ContainerPlan that = (ContainerPlan) o;

      if (containerId != that.containerId) return false;
      if (taskInstancePlan != null ? !taskInstancePlan.equals(that.taskInstancePlan) : that.taskInstancePlan != null)
        return false;
      return (requiredresource != null ? requiredresource.equals(that.requiredresource) : that.requiredresource == null) && (scheduledresource != null ? scheduledresource.equals(that.scheduledresource) : that.scheduledresource == null);
    }

    @Override
    public int hashCode() {
      int result = containerId;
      result = 31 * result + (taskInstancePlan != null ? taskInstancePlan.hashCode() : 0);
      result = 31 * result + (requiredresource != null ? requiredresource.hashCode() : 0);
      result = 31 * result + (scheduledresource != null ? scheduledresource.hashCode() : 0);
      return result;
    }

  }

}
