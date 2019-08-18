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

package edu.iu.dsc.tws.api.compute.executor;

import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskSchedulePlan;
import edu.iu.dsc.tws.api.config.Config;

public interface IExecutionPlanBuilder {
  /**
   * Schedule and execution
   * @param taskGraph the task graph
   * @param taskSchedule the task schedule
   * @return the execution created and null if nothing to execute
   */
  ExecutionPlan build(Config cfg, ComputeGraph taskGraph,
                      TaskSchedulePlan taskSchedule);
}
