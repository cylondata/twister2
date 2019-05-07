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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.task.api.OutputCollection;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.TaskMessage;
import edu.iu.dsc.tws.task.api.schedule.ContainerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class TaskContextImpl implements TaskContext {
  /**
   * Task index, which goes from 0 up to the number of parallel tasks
   */
  private int taskIndex;

  /**
   * The task id for each task, each instance of the same task will have the same id
   */
  private int taskId;

  /**
   * Unique id of the task
   */
  private int globalTaskId;

  /**
   * Name of the task
   */
  private String taskName;

  /**
   * Parallel instances of the task
   */
  private int parallelism;

  /**
   * Collect output
   */
  private OutputCollection collection;

  /**
   * Task specific configurations
   */
  private Map<String, Object> configs;

  /**
   * The worker id this task instance belongs to
   */
  private int workerId;

  /**
   * Keep track of the edges that are been done
   */
  private Map<String, Boolean> isDone = new HashMap<>();

  /**
   * The outgoing streams from this task
   */
  private Map<String, String> outEdges;

  /**
   * The incoming edges and the tasks connected to them
   */
  private Map<String, String> inputs;

  private TaskSchedulePlan taskSchedulePlan;

  /**
   * Names of out edges
   */
  private Set<String> outEdgeNames = new HashSet<>();

  private TaskContextImpl(int taskIndex, int taskId, int globalTaskId, String taskName,
                          int parallelism, int wId,
                          Map<String, Object> configs,
                          TaskSchedulePlan taskSchedulePlan) {
    this.taskIndex = taskIndex;
    this.globalTaskId = globalTaskId;
    this.taskId = taskId;
    this.taskName = taskName;
    this.parallelism = parallelism;
    this.workerId = wId;
    this.configs = configs;
    this.taskSchedulePlan = taskSchedulePlan;
  }


  public TaskContextImpl(int taskIndex, int taskId, int globalTaskId, String taskName,
                         int parallelism, int wId, Map<String, Object> configs,
                         Map<String, String> inputs, TaskSchedulePlan taskSchedulePlan) {
    this(taskIndex, taskId, globalTaskId, taskName, parallelism, wId, configs, taskSchedulePlan);
    this.inputs = inputs;
  }

  public TaskContextImpl(int taskIndex, int taskId, int globalTaskId,
                         String taskName, int parallelism, int wId,
                         OutputCollection collection, Map<String, Object> configs,
                         Map<String, String> outEdges, TaskSchedulePlan taskSchedulePlan) {
    this(taskIndex, taskId, globalTaskId, taskName, parallelism, wId, configs, taskSchedulePlan);
    this.collection = collection;
    this.outEdges = outEdges;
    outEdgeNames.addAll(outEdges.keySet());
  }

  public TaskContextImpl(int taskIndex, int taskId, int globalTaskId, String taskName,
                         int parallelism, int wId,
                         OutputCollection collection, Map<String, Object> configs,
                         Map<String, String> inputs, Map<String, String> outEdges,
                         TaskSchedulePlan taskSchedulePlan) {
    this(taskIndex, taskId, globalTaskId, taskName, parallelism, wId, collection,
        configs, outEdges, taskSchedulePlan);
    this.inputs = inputs;
  }

  @Override
  public Set<ContainerPlan> getWorkers() {
    return this.taskSchedulePlan.getContainers();
  }

  @Override
  public Map<Integer, ContainerPlan> getWorkersMap() {
    return this.taskSchedulePlan.getContainersMap();
  }

  /**
   * Reset the context
   */
  public void reset() {
    this.isDone = new HashMap<>();
  }

  /**
   * The task index
   *
   * @return index
   */
  public int taskIndex() {
    return taskIndex;
  }

  /**
   * Globally unique id
   *
   * @return the task id
   */
  public int globalTaskId() {
    return globalTaskId;
  }

  /**
   * Get the task id for this task
   *
   * @return task id
   */
  public int taskId() {
    return taskId;
  }

  /**
   * Name of the task
   */
  public String taskName() {
    return taskName;
  }

  /**
   * Get the parallism of the task
   *
   * @return number of parallel instances
   */
  public int getParallelism() {
    return parallelism;
  }

  /**
   * Get the worker id this task is running
   *
   * @return worker id
   */
  public int getWorkerId() {
    return workerId;
  }

  /**
   * Get the task specific configurations
   *
   * @return map of configurations
   */
  public Map<String, Object> getConfigurations() {
    return configs;
  }

  /**
   * Get a configuration with a name
   *
   * @param name name of the config
   * @return the config, if not found return null
   */
  public Object getConfig(String name) {
    return configs.get(name);
  }

  /**
   * Get the out edges of this task
   *
   * @return the output edges, edge name and task connected to this edge
   */
  public Map<String, String> getOutEdges() {
    return outEdges;
  }

  /**
   * Get the edge names and the tasks connected using those edges
   *
   * @return a map with edge, and task connected to this edge
   */
  public Map<String, String> getInputs() {
    return inputs;
  }

  private void validateEdge(String edge) {
    if (!outEdgeNames.contains(edge)) {
      throw new RuntimeException("output on edge not specified by user: " + edge);
    }

    if (isDone.containsKey(edge) && isDone.get(edge)) {
      throw new RuntimeException("Cannot send on a stream that ended");
    }
  }

  /**
   * Write a message with a key
   *
   * @param edge the edge
   * @param key key
   * @param message message
   * @return true if the message is accepted
   */
  public boolean write(String edge, Object key, Object message) {
    this.validateEdge(edge);
    return collection.collect(edge, new TaskMessage(key, message, edge, globalTaskId));
  }

  /**
   * Write a message to the destination
   *
   * @param edge edge
   * @param message message
   */
  public boolean write(String edge, Object message) {
    return write(edge, null, message);
  }

  /**
   * Write the last message
   *
   * @param edge edge
   * @param message message
   */
  public boolean writeEnd(String edge, Object message) {
    return this.writeEnd(edge, null, message);
  }

  /**
   * Write the last message
   *
   * @param edge edge
   * @param key key
   * @param message message
   */
  public boolean writeEnd(String edge, Object key, Object message) {
    this.validateEdge(edge);
    boolean collect = collection.collect(edge, new TaskMessage(key, message, edge, globalTaskId));
    isDone.put(edge, true);
    return collect;
  }

  /**
   * End the current writing
   *
   * @param edge edge
   */
  public void end(String edge) {
    isDone.put(edge, true);
  }

  /**
   * Return true, if this task is done
   *
   * @param edge edge name
   * @return boolean
   */
  public boolean isDone(String edge) {
    return isDone.containsKey(edge) && isDone.get(edge);
  }
}
