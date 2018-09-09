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
package edu.iu.dsc.tws.executor.core;

import java.util.HashSet;
import java.util.Set;

import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

/**
 * This is a global task id generator depending on the taskId, task index and task name
 */
public class TaskIdGenerator {

  /**
   * Generate a unique global task id
   * @param taskName name of the task
   * @param taskId task id
   * @param taskIndex task index
   * @return the global task id
   */
  public int generateGlobalTaskId(String taskName, int taskId, int taskIndex) {
    return taskId * 100000 + taskIndex;
  }

  public Set<Integer> getTaskIds(String taskName, int taskId, DataFlowTaskGraph graph) {
    Vertex v = graph.vertex(taskName);
    int parallel = v.getParallelism();

    Set<Integer> sources = new HashSet<>();
    for (int i = 0; i < parallel; i++) {
      sources.add(generateGlobalTaskId(taskName, taskId, i));
    }
    return sources;
  }

  public Set<Integer> getTaskIdsOfContainer(TaskSchedulePlan.ContainerPlan cPlan) {
    Set<TaskSchedulePlan.TaskInstancePlan> ips = cPlan.getTaskInstances();
    Set<Integer> tasks = new HashSet<>();
    for (TaskSchedulePlan.TaskInstancePlan ip : ips) {
      tasks.add(generateGlobalTaskId(ip.getTaskName(), ip.getTaskId(), ip.getTaskIndex()));
    }
    return tasks;
  }
}
