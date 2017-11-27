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
import twister2.proto.system.TaskSchedulingPlans.InstanceTaskPlan;
import twister2.proto.system.TaskSchedulingPlans.ContainerPlan;

public class TaskSchedulePlanDeSerializer {

  public TaskSchedulePlan TaskScheduleProto(TaskSchedulingPlans.TaskSchedulePlan taskSchedulePlan) {
    Set<TaskSchedulePlan.ContainerPlan> containerPlanSet = new HashSet<>();
    for (TaskSchedulingPlans.ContainerPlan containerPlan : taskSchedulePlan.getContainerplanList()) {
      containerPlanSet.add(convert(containerPlan));
    }
    return new TaskSchedulePlan(taskSchedulePlan.getId(), containerPlanSet);
  }

  private TaskSchedulePlan.ContainerPlan convert(TaskSchedulingPlans.ContainerPlan containerPlan) {
    Set<TaskSchedulePlan.TaskInstancePlan> taskInstancePlanSet = new HashSet<>();
    for (TaskSchedulingPlans.InstanceTaskPlan instanceTaskPlan : containerPlan.getInstancetaskplanList()) {
      taskInstancePlanSet.add(convert(instanceTaskPlan));
    }
    return new TaskSchedulePlan.ContainerPlan(containerPlan.getId(), taskInstancePlanSet,
        convert(containerPlan.getRequiredresource()),
        convert(containerPlan.getScheduledresource()));
  }

  private TaskSchedulePlan.TaskInstancePlan convert(TaskSchedulingPlans.InstanceTaskPlan instanceTaskPlan) {

    return new TaskSchedulePlan.TaskInstancePlan(new TaskInstanceId(instanceTaskPlan.getTaskname(),
        instanceTaskPlan.getTaskid(),
        instanceTaskPlan.getTaskindex()),
        convert(instanceTaskPlan.getResource()));
  }

  private Resource convert(TaskSchedulingPlans.Resource requiredresource) {
    Resource resource = null;
    if (requiredresource != null && requiredresource.isInitialized()) {
      resource = new Resource(resource.getRam(), resource.getDisk(), resource.getCpu());
    }
    return resource;
  }
}
