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

import java.util.HashSet;
import java.util.logging.Logger;

import edu.iu.dsc.tws.tsched.spi.scheduler.TaskSchedulerException;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ScheduleException;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class Container {

  private static final Logger LOG = Logger.getLogger(Container.class.getName());
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

  void add(TaskSchedulePlan.TaskInstancePlan taskInstancePlan) {
    if (this.taskInstances.contains(taskInstancePlan)) {
      throw new ScheduleException(String.format(
          "Instance %s already exists in container %s", taskInstancePlan, toString()));
    }
    assertHasSpace(taskInstancePlan.getResource());
    this.taskInstances.add(taskInstancePlan);
    /*boolean flag = assertHasSpace(taskInstancePlan.getResource());
    if (flag) {
      this.taskInstances.add(taskInstancePlan);
    }*/
  }

  private void assertHasSpace(Resource resourceValue) throws TaskSchedulerException {

    boolean flag = false;
    Resource usedResources = this.getTotalUsedResources();

    double newRam = usedResources.getRam() + resourceValue.getRam() + paddingPercentage;
    double newDisk = usedResources.getDisk() + resourceValue.getDisk() + paddingPercentage;
    double newCpu = usedResources.getCpu() + resourceValue.getCpu() + paddingPercentage;

    LOG.info("New Ram Value:" + newRam + "\t "
        + "Resource Value Ram:" + this.resource.getRam() + "\n");
    LOG.info("New Disk Value:" + newDisk + "\t "
        + "Resource Value Disk:" + this.resource.getDisk() + "\n");
    LOG.info("New Cpu Value:" + newCpu + "\t"
        + "Resource Value Cpu:" + this.resource.getCpu() + "\n");

    if (newRam > this.resource.getRam()) {
      try {
        throw new TaskSchedulerException(String.format("Adding %s bytes of ram to existing %s "
                + "bytes with %d percent padding would exceed capacity %s",
            resourceValue.getRam(), usedResources.getRam(),
            paddingPercentage, this.resource.getRam()));
      } catch (TaskSchedulerException e) {
        e.printStackTrace();
      }
      //flag = true;
    }
    if (newDisk > this.resource.getDisk()) {
      try {
        throw new TaskSchedulerException(String.format("Adding %s bytes of disk to existing %s "
                + "bytes with %s percent padding would exceed capacity %s",
            resourceValue.getDisk(), usedResources.getDisk(),
            paddingPercentage, this.resource.getDisk()));
      } catch (TaskSchedulerException e) {
        e.printStackTrace();
      }
      //flag = true;
    }
    if (newCpu > this.resource.getCpu()) {
      try {
        throw new TaskSchedulerException(String.format("Adding %s cores to existing %s "
                + "cores with %d percent padding would exceed capacity %s",
            resourceValue.getCpu(), usedResources.getCpu(),
            paddingPercentage, this.resource.getCpu()));
      } catch (TaskSchedulerException e) {
        e.printStackTrace();
      }
      //flag = true;
    }
    //return flag;
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
}


