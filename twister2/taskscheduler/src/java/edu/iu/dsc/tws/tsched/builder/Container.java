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

import java.util.HashSet;

import edu.iu.dsc.tws.tsched.spi.scheduler.TaskSchedulerException;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ScheduleException;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

/**
 * This class is for the logical representation of container and it has getter and setter
 * property to retrieve and set the task instances allocated to the container.
 *
 */
public class Container {

  private int containerId;
  private HashSet<TaskSchedulePlan.TaskInstancePlan> taskInstances;
  private Resource resource;
  private int paddingPercentage;

  public Container(int containerId,
                   Resource containerMaximumResourceValue, int requestedContainerPadding) {
    this.containerId = containerId;
    this.resource = containerMaximumResourceValue;
    this.taskInstances = new HashSet<>();
    this.paddingPercentage = requestedContainerPadding;
  }

  public int getContainerId() {
    return containerId;
  }

  public void setContainerId(int containerId) {
    this.containerId = containerId;
  }

  public HashSet<TaskSchedulePlan.TaskInstancePlan> getTaskInstances() {
    return taskInstances;
  }

  public void setTaskInstances(HashSet<TaskSchedulePlan.TaskInstancePlan> taskInstances) {
    this.taskInstances = taskInstances;
  }

  public Resource getResource() {
    return resource;
  }

  public void setResource(Resource resource) {
    this.resource = resource;
  }

  public int getPaddingPercentage() {
    return paddingPercentage;
  }

  public void setPaddingPercentage(int paddingPercentage) {
    this.paddingPercentage = paddingPercentage;
  }

  /**
   * It will add the task instance plan to the container if the required and available resource
   * meets the requirements. It will call @assertHasSpace to validate that whether the resource
   * satisfies the required value.
   *
   * @param taskInstancePlan
   * @throws TaskSchedulerException
   */
  void add(TaskSchedulePlan.TaskInstancePlan taskInstancePlan) throws TaskSchedulerException {
    if (this.taskInstances.contains(taskInstancePlan)) {
      throw new ScheduleException(String.format(
          "Instance %s already exists in container %s", taskInstancePlan, toString()));
    }
    assertHasSpace(taskInstancePlan.getResource());
    this.taskInstances.add(taskInstancePlan);
  }


  private void assertHasSpace(Resource resourceValue) throws TaskSchedulerException {

    Resource usedResources = this.getTotalUsedResources();

    double newRam = usedResources.getRam() + resourceValue.getRam() + paddingPercentage;
    double newDisk = usedResources.getDisk() + resourceValue.getDisk() + paddingPercentage;
    double newCpu = usedResources.getCpu() + resourceValue.getCpu() + paddingPercentage;

    if (newRam > this.resource.getRam()) {
      throw new TaskSchedulerException(String.format("Adding %s bytes of ram to existing %s "
              + "bytes with %d percent padding would exceed capacity %s",
          resourceValue.getRam(), usedResources.getRam(),
          paddingPercentage, this.resource.getRam()));
    }
    if (newDisk > this.resource.getDisk()) {
      throw new TaskSchedulerException(String.format("Adding %s bytes of disk to existing %s "
              + "bytes with %s percent padding would exceed capacity %s",
          resourceValue.getDisk(), usedResources.getDisk(),
          paddingPercentage, this.resource.getDisk()));
    }
    if (newCpu > this.resource.getCpu()) {
      throw new TaskSchedulerException(String.format("Adding %s cores to existing %s "
              + "cores with %d percent padding would exceed capacity %s",
          resourceValue.getCpu(), usedResources.getCpu(),
          paddingPercentage, this.resource.getCpu()));
    }
  }

  private Resource getTotalUsedResources() {
    double usedRam = 0.0;
    double usedCpuCores = 0.0;
    double usedDisk = 0.0;
    for (TaskSchedulePlan.TaskInstancePlan instancePlan : this.taskInstances) {
      Resource instancePlanResource = instancePlan.getResource();
      usedRam += instancePlanResource.getRam();
      usedCpuCores += instancePlanResource.getCpu();
      usedDisk += instancePlanResource.getDisk();
    }
    return new Resource(usedRam, usedDisk, usedCpuCores);
  }

  @Override
  public String toString() {
    return String.format("{containerId=%s, instances=%s, resource=%s, paddingPercentage=%s}",
        containerId, taskInstances, resource, paddingPercentage);
  }
}


