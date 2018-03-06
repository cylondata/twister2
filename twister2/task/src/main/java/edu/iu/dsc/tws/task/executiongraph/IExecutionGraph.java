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
package edu.iu.dsc.tws.task.executiongraph;

import java.util.Set;

//import edu.iu.dsc.tws.task.api.Task;

//import edu.iu.dsc.tws.task.core.TaskExecutorFixedThread;
import edu.iu.dsc.tws.task.taskgraphbuilder.TaskGraphMapper;
//import edu.iu.dsc.tws.task.taskgraphbuilder.TaskExecutor;

/**
 * This is the main interface for generating the execution graph. Once the executor
 * works fine, the method which returns TaskExecutorFixedThread should be invoked...
 */

public interface IExecutionGraph {

  /*String generateExecutionGraph(int containerId, Set<Task> processedTaskSet);
  TaskExecutorFixedThread generateExecutionGraph(int containerId,
                                             Set<Task> processedTaskSet);*/

  /* String generateExecutionGraph(int containerId);
  TaskExecutorFixedThread generateExecutionGraph(int containerId);*/

  String generateExecutionGraph(int containerId);

  String generateExecutionGraph(int containerId, Set<TaskGraphMapper> processedTaskSet);

}
