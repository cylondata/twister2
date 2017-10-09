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

import java.util.HashSet;
import java.util.Set;

import twister2.proto.system.TaskSchedulingPlans;

public class TaskSchedulePlanSerializer {

  public TaskSchedulingPlans.TaskSchedulePlan TaskScheduleProto(TaskSchedulePlan taskschedulePlan) {
    TaskSchedulingPlans.TaskSchedulePlan.Builder builder = TaskSchedulingPlans.TaskSchedulePlan.newBuilder()
        .setId(taskschedulePlan.getJobId());

    for (TaskSchedulePlan.ContainerPlan containerPlan : taskschedulePlan.getContainers()) {
      builder.addContainerplan(builder(containerPlan));
    }

    return builder.build();
  }

  private TaskSchedulingPlans.ContainerPlan.Builder builder(TaskSchedulePlan.ContainerPlan containerPlan) {
    TaskSchedulingPlans.ContainerPlan.Builder builder = TaskSchedulingPlans.ContainerPlan.newBuilder()
        .setId(containerPlan.getContainerId())
        .setRequiredresource(builder(containerPlan.getRequiredResource()));

    if (containerPlan.getScheduledResource().isPresent()) {
      builder.setScheduledresource(builder(containerPlan.getScheduledResource().get()));
    }

    for (TaskSchedulePlan.TaskInstancePlan taskInstancePlan : containerPlan.getTaskInstances()) {
      builder.addInstancetaskplan(builder(taskInstancePlan));
    }

    return builder;
  }

  private TaskSchedulingPlans.InstanceTaskPlan.Builder builder(TaskSchedulePlan.TaskInstancePlan instancePlan) {
    return TaskSchedulingPlans.InstanceTaskPlan.newBuilder()
        .setTaskname(instancePlan.getTaskName())
        .setTaskid(instancePlan.getTaskId())
        .setTaskindex(String.valueOf(instancePlan.getTaskIndex()))
        .setResource(builder(instancePlan.getResource()));
  }

  private TaskSchedulingPlans.Resource.Builder builder(Resource resource) {
    return TaskSchedulingPlans.Resource.newBuilder()
        .setAvailableCPU(resource.getCpu())
        .setAvailableMemory(resource.getRam())
        .setAvailableDisk(resource.getDisk());
  }
}
