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

import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedule;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;

import java.util.TreeSet;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public  class TaskSchedulePlanBuilder {

  private int jobId;
  private int numNodes;
  private ArrayList<Integer> taskIds;
  private TaskSchedulePlan previousTaskSchedulePlan;
  private Resource instanceDefaultResourceValue;
  private Resource containerMaximumResourceValue;
  private Map<String, Double> componentRamMap;
  private int requestedContainerPadding;
  private int numContainers;

  public TaskSchedulePlanBuilder(int jobId) {
    this.jobId = jobId;
    this.previousTaskSchedulePlan = null;
  }

  public TaskSchedulePlanBuilder(int jobId, TaskSchedulePlan previousTaskSchedulePlan) {
    this.jobId = jobId;
    this.previousTaskSchedulePlan = previousTaskSchedulePlan;
    this.numContainers = 0;
    this.requestedContainerPadding = 0;
    this.componentRamMap = new HashMap<> ();
  }

  public void newTaskSchedulePlanBuilder(){

  }

  public int getJobId() {
    return jobId;
  }

  public void setJobId(int jobId) {
    this.jobId = jobId;
  }

  public int getNumNodes() {
    return numNodes;
  }

  public void setNumNodes(int numNodes) {
    this.numNodes = numNodes;
  }

  public ArrayList<Integer> getTaskIds() {
    return taskIds;
  }

  public void setTaskIds(ArrayList<Integer> taskIds) {
    this.taskIds = taskIds;
  }

  public TaskSchedulePlan getPreviousTaskSchedulePlan() {
    return previousTaskSchedulePlan;
  }

  public TaskSchedulePlan setPreviousTaskSchedulePlan(TaskSchedulePlan previousTaskSchedulePlan) {
    this.previousTaskSchedulePlan = previousTaskSchedulePlan;
    return this.previousTaskSchedulePlan;
  }

  public Resource getInstanceDefaultResourceValue() {
    return instanceDefaultResourceValue;
  }

  /*public TaskSchedulePlan setInstanceDefaultResourceValue(Resource instanceDefaultResourceValue) {
    this.instanceDefaultResourceValue = instanceDefaultResourceValue;
    return this;

  }*/

  public Resource getContainerMaximumResourceValue() {
    return containerMaximumResourceValue;
  }

  public void setContainerMaximumResourceValue(Resource containerMaximumResourceValue) {
    this.containerMaximumResourceValue = containerMaximumResourceValue;
  }

  public Map<String, Double> getComponentRamMap() {
    return componentRamMap;
  }

  public void setComponentRamMap(Map<String, Double> componentRamMap) {
    this.componentRamMap = componentRamMap;
  }

  public int getRequestedContainerPadding() {
    return requestedContainerPadding;
  }

  public void setRequestedContainerPadding(int requestedContainerPadding) {
    this.requestedContainerPadding = requestedContainerPadding;
  }

  public int getNumContainers() {
    return numContainers;
  }

  public void setNumContainers(int numContainers) {
    this.numContainers = numContainers;
  }
}
