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
package edu.iu.dsc.tws.task.api.schedule;

import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * This class is responsible for assigning the container id, task instances, required
 * resource, and scheduled resource for the task instances.
 */
public class ContainerPlan implements Comparable<ContainerPlan> {
  private final int containerId;
  private final Set<TaskInstancePlan> taskInstances;
  private final Resource requiredResource;
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<Resource> scheduledResource;

  public ContainerPlan(int id, Set<TaskInstancePlan> instances, Resource requiredResource) {
    this(id, instances, requiredResource, null);
  }

  public ContainerPlan(int id,
                       Set<TaskInstancePlan> taskInstances,
                       Resource requiredResource,
                       Resource scheduledResource) {
    this.containerId = id;
    //this.taskInstances = ImmutableSet.copyOf(taskInstances);
    this.taskInstances = new TreeSet<>(taskInstances);
    this.requiredResource = requiredResource;
    this.scheduledResource = Optional.ofNullable(scheduledResource);
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
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ContainerPlan that = (ContainerPlan) o;

    return containerId == that.containerId
        && getRequiredResource().equals(that.getRequiredResource())
        && getScheduledResource().equals(that.getScheduledResource());
  }


  @Override
  public int hashCode() {
    int result = containerId;
    result = 31 * result + getTaskInstances().hashCode();
    result = 31 * result + getRequiredResource().hashCode();
    if (scheduledResource.isPresent()) {
      result = (31 * result) + getScheduledResource().get().hashCode();
    }
    return result;
  }

  @Override
  public int compareTo(ContainerPlan o) {
    return Integer.compare(this.containerId, o.containerId);
  }
}
