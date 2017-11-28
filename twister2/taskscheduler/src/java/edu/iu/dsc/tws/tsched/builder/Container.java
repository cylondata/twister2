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

import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class Container {

  private int containerId;
  private HashSet<TaskSchedulePlan.TaskInstancePlan> taskInstances;
  private Resource resource;
  private int paddingPercentage;

  public Container(int containerId,
                   Resource containerMaximumResourceValue, int requestedContainerPadding) {
    this.containerId = containerId;
    this.resource = containerMaximumResourceValue;
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
    //assertHasSpace(instancePlan.getResource());
    this.taskInstances.add(taskInstancePlan);
  }

}

