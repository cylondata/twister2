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

package edu.iu.dsc.tws.api.compute.schedule;

import edu.iu.dsc.tws.api.compute.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskSchedulePlan;
import edu.iu.dsc.tws.api.compute.schedule.elements.WorkerPlan;
import edu.iu.dsc.tws.api.config.Config;

/**
 * This is the main interface for the task scheduler.
 */
public interface ITaskScheduler {

  void initialize(Config cfg);

  void initialize(Config cfg, int workerId);

  TaskSchedulePlan schedule(DataFlowTaskGraph graph, WorkerPlan workerPlan);
}
