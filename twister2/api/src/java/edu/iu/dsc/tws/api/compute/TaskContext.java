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
package edu.iu.dsc.tws.api.compute;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskInstancePlan;
import edu.iu.dsc.tws.api.compute.schedule.elements.WorkerSchedulePlan;

/**
 * <code>TaskContext</code> is passed to the task instances when they are being prepared to
 * execute. This class is used for getting information about the task as well as sending messages.
 */
public interface TaskContext {
  /**
   * Reset the context
   */
  void reset();

  /**
   * The task index
   *
   * @return index
   */
  int taskIndex();

  /**
   * Task id
   *
   * @return the task id
   */
  int globalTaskId();

  /**
   * Get the task id for this task
   *
   * @return task id
   */
  int taskId();

  /**
   * Name of the task
   */
  String taskName();

  /**
   * Get the parallism of the task
   *
   * @return number of parallel instances
   */
  int getParallelism();

  /**
   * Get the worker id this task is running
   *
   * @return worker id
   */
  int getWorkerId();

  /**
   * Get the task specific configurations
   *
   * @return map of configurations
   */
  Map<String, Object> getConfigurations();

  /**
   * Get a configuration with a name
   *
   * @param name name of the config
   * @return the config, if not found return null
   */
  Object getConfig(String name);

  /**
   * Get the out edges of this task
   *
   * @return the output edges
   */
  Map<String, String> getOutEdges();

  /**
   * Get the in edges of the task. Generally, each edge would have a single source. But, the join
   * edges have multiple sources. These will be reflected by the Set.
   *
   * @return a map with edge name -> Set of source task names for that edge
   */
  Map<String, Set<String>> getInEdges();

  /**
   * Write a message with a key
   *
   * @param edge the edge
   * @param key key
   * @param message message
   * @return true if the message is accepted
   */
  boolean write(String edge, Object key, Object message);

  /**
   * Write a message to the destination
   *
   * @param edge edge
   * @param message message
   */
  boolean write(String edge, Object message);

  /**
   * Write a barrier message to the destination
   *
   * @param edge edge
   * @param message message
   */
  boolean writeBarrier(String edge, Object message);

  /**
   * Write the last message
   *
   * @param edge edge
   * @param message message
   */
  boolean writeEnd(String edge, Object message);

  /**
   * Write the last message
   *
   * @param edge edge
   * @param key key
   * @param message message
   */
  boolean writeEnd(String edge, Object key, Object message);

  /**
   * End the current writing
   *
   * @param edge edge
   */
  void end(String edge);

  /**
   * Ends all the edges
   */
  void endAll();

  /**
   * Return true, if this task is done
   *
   * @param edge edge name
   * @return boolean
   */
  boolean isDone(String edge);

  /**
   * Set of workers in the current topology
   */
  Set<WorkerSchedulePlan> getWorkers();

  /**
   * Map of worker where value is the container id
   */
  Map<Integer, WorkerSchedulePlan> getWorkersMap();

  /**
   * Get the container of this task
   */
  default WorkerSchedulePlan getWorker() {
    return this.getWorkersMap().get(this.getWorkerId());
  }

  /**
   * Instances of tasks for the given name
   */
  default Set<TaskInstancePlan> getTasksByName(String name) {
    return this.getWorkers().stream().flatMap(
        worker -> worker.getTaskInstances().stream()
    ).filter(
        taskInstancePlan -> taskInstancePlan.getTaskName().equals(name)
    ).collect(Collectors.toSet());
  }

  /**
   * Tasks of given type which have been scheduled in this worker
   */
  default Set<TaskInstancePlan> getTasksInThisWorkerByName(String name) {
    return this.getWorker().getTaskInstances().stream()
        .filter(taskInstancePlan -> taskInstancePlan.getTaskName().equals(name))
        .collect(Collectors.toSet());
  }

  /**
   * Get the operation mode of the task graph
   * @return the operation mode
   */
  OperationMode getOperationMode();
}
