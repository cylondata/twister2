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
package edu.iu.dsc.tws.executor.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public final class TaskPlanBuilder {
  private TaskPlanBuilder() {
  }

  /**
   * Create a task plan based on the resource plan from resources and scheduled plan
   * @param resourcePlan resource plan
   * @param schedulePlan schedule plan
   * @param idGenerator global task id generator
   * @return the task plan
   */
  public static TaskPlan build(AllocatedResources resourcePlan,
                               TaskSchedulePlan schedulePlan, TaskIdGenerator idGenerator) {
    Set<TaskSchedulePlan.ContainerPlan> cPlanList = schedulePlan.getContainers();
    Map<Integer, Set<Integer>> containersToTasks = new HashMap<>();
    Map<Integer, Set<Integer>> groupsToTasks = new HashMap<>();

    for (TaskSchedulePlan.ContainerPlan c : cPlanList) {
      Set<TaskSchedulePlan.TaskInstancePlan> tSet = c.getTaskInstances();
      Set<Integer> instances = new HashSet<>();

      for (TaskSchedulePlan.TaskInstancePlan tPlan : tSet) {
        instances.add(idGenerator.generateGlobalTaskId(tPlan.getTaskName(),
            tPlan.getTaskId(), tPlan.getTaskIndex()));
      }
      containersToTasks.put(c.getContainerId(), instances);
    }

    List<WorkerComputeResource> containers = resourcePlan.getWorkerComputeResources();
    Map<String, List<WorkerComputeResource>> containersPerNode = new HashMap<>();
    for (WorkerComputeResource c : containers) {
      String name = Integer.toString(c.getId());
      List<WorkerComputeResource> containerList;
      if (!containersPerNode.containsKey(name)) {
        containerList = new ArrayList<>();
        containersPerNode.put(name, containerList);
      } else {
        containerList = containersPerNode.get(name);
      }
      containerList.add(c);
    }

    int i = 0;
    // we take each container as an executor
    for (Map.Entry<String, List<WorkerComputeResource>> e : containersPerNode.entrySet()) {
      Set<Integer> executorsOfGroup = new HashSet<>();
      for (WorkerComputeResource c : e.getValue()) {
        executorsOfGroup.add(c.getId());
      }
      groupsToTasks.put(i, executorsOfGroup);
      i++;
    }

    return new TaskPlan(containersToTasks, groupsToTasks, resourcePlan.getWorkerId());
  }
}
