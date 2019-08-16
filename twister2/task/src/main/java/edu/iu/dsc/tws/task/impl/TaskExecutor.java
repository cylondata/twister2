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
package edu.iu.dsc.tws.task.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.executor.IExecution;
import edu.iu.dsc.tws.api.compute.executor.INodeInstance;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.compute.nodes.ISink;
import edu.iu.dsc.tws.api.compute.nodes.ISource;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskSchedulePlan;
import edu.iu.dsc.tws.api.compute.schedule.elements.Worker;
import edu.iu.dsc.tws.api.compute.schedule.elements.WorkerPlan;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.EmptyDataObject;
import edu.iu.dsc.tws.executor.core.ExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.threading.Executor;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler;
import edu.iu.dsc.tws.tsched.taskscheduler.TaskScheduler;

/**
 * The task executor API, this class can be used to create an execution plan and execute
 */
public class TaskExecutor {
  private static final Logger LOG = Logger.getLogger(TaskExecutor.class.getName());

  /**
   * Configuration
   */
  private Config config;

  /**
   * Worker id
   */
  private int workerID;

  /**
   * The allocated resources
   */
  private List<JobMasterAPI.WorkerInfo> workerInfoList;

  /**
   * The network communicator
   */
  private Communicator communicator;
  private CheckpointingClient checkpointingClient;

  /**
   * The executor used by this task executor
   */
  private Executor executor;

  /**
   * Creates a task executor.
   *
   * @param cfg the configuration
   * @param wId the worker id
   * @param net communicator
   */
  public TaskExecutor(Config cfg, int wId, List<JobMasterAPI.WorkerInfo> workerInfoList,
                      Communicator net, CheckpointingClient checkpointingClient) {
    this.config = cfg;
    this.workerID = wId;
    this.workerInfoList = workerInfoList;
    this.communicator = net;
    this.checkpointingClient = checkpointingClient;
  }

  public TaskExecutor(WorkerEnvironment workerEnv) {
    this.config = workerEnv.getConfig();
    this.workerID = workerEnv.getWorkerId();
    this.workerInfoList = workerEnv.getWorkerList();
    this.communicator = workerEnv.getCommunicator();
    this.checkpointingClient = workerEnv.getWorkerController().getCheckpointingClient();
  }

  /**
   * Create an execution plan from the given graph
   *
   * @param graph task graph
   * @return the data set
   */
  public ExecutionPlan plan(ComputeGraph graph) {

    RoundRobinTaskScheduler roundRobinTaskScheduler = new RoundRobinTaskScheduler();
    roundRobinTaskScheduler.initialize(config);

    TaskScheduler taskScheduler = new TaskScheduler();
    taskScheduler.initialize(config);

    WorkerPlan workerPlan = createWorkerPlan();

    TaskSchedulePlan taskSchedulePlan = roundRobinTaskScheduler.schedule(graph, workerPlan);
    //TaskSchedulePlan taskSchedulePlan = taskScheduler.schedule(graph, workerPlan);

    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(
        workerID, workerInfoList, communicator, this.checkpointingClient);
    return executionPlanBuilder.build(config, graph, taskSchedulePlan);
  }

  /**
   * Execute a plan and a graph. This call blocks until the execution finishes. In case of
   * streaming, this call doesn't return while for batch computations it returns after
   * the execution is done.
   *
   * @param taskConfig the user configuration to be passed to the task instances
   * @param graph the dataflow graph
   * @param plan the execution plan
   */
  public void execute(Config taskConfig, ComputeGraph graph, ExecutionPlan plan) {
    Config newCfg = Config.newBuilder().putAll(config).putAll(taskConfig).build();

    if (executor == null) {
      executor = new Executor(newCfg, workerID, communicator.getChannel(),
          graph.getOperationMode());
    }
    executor.execute(plan);
    executor.waitFor(plan);
  }

  /**
   * Execute a plan and a graph. This call blocks until the execution finishes. In case of
   * streaming, this call doesn't return while for batch computations it returns after
   * the execution is done.
   *
   * @param graph the dataflow graph
   * @param plan the execution plan
   */
  public void execute(ComputeGraph graph, ExecutionPlan plan) {
    if (executor == null) {
      executor = new Executor(config, workerID, communicator.getChannel(),
          graph.getOperationMode());
    }
    executor.execute(plan);
    executor.waitFor(plan);
  }

  /**
   * Execute a plan and a graph. This call blocks until the execution finishes. In case of
   * streaming, this call doesn't return while for batch computations it returns after
   * the execution is done.
   *
   * @param graph the dataflow graph
   * @param plan the execution plan
   */
  public void itrExecute(ComputeGraph graph, ExecutionPlan plan) {
    if (executor == null) {
      executor = new Executor(config, workerID, communicator.getChannel(),
          graph.getOperationMode());
    }
    executor.execute(plan);
  }

  /**
   * Wait for the execution to complete
   *
   * @param plan the dataflow graph
   * @param graph the task graph
   */
  public void waitFor(ComputeGraph graph, ExecutionPlan plan) {
    if (executor == null) {
      throw new IllegalStateException("Cannot call waifor before calling execute");
    }
    executor.waitFor(plan);
  }

  /**
   * Execute a plan and a graph. This call blocks until the execution finishes. In case of
   * streaming, this call doesn't return while for batch computations it returns after
   * the execution is done.
   *
   * @param graph the dataflow graph
   * @param plan the execution plan
   */
  public IExecution iExecute(ComputeGraph graph, ExecutionPlan plan) {
    if (executor == null) {
      executor = new Executor(config, workerID, communicator.getChannel(),
          graph.getOperationMode());
    }
    return executor.iExecute(plan);
  }

  /**
   * Add input to the the task instances
   *
   * @param graph task graph
   * @param plan execution plan
   * @param taskName task name
   * @param inputKey inputkey
   * @param input input
   */
  public void addInput(ComputeGraph graph, ExecutionPlan plan,
                       String taskName, String inputKey, DataObject<?> input) {
    Map<Integer, INodeInstance> nodes = plan.getNodes(taskName);
    if (nodes == null) {
      return;
    }

    for (Map.Entry<Integer, INodeInstance> e : nodes.entrySet()) {
      INodeInstance node = e.getValue();
      INode task = node.getNode();
      if (task instanceof Receptor) {
        ((Receptor) task).add(inputKey, input);
      } else {
        throw new RuntimeException("Cannot add input to non input instance: " + node);
      }
    }
  }

  /**
   * Add input to the the task instances
   *
   * @param graph task graph
   * @param plan execution plan
   * @param inputKey inputkey
   * @param input input
   */
  public void addSourceInput(ComputeGraph graph, ExecutionPlan plan,
                             String inputKey, DataObject<Object> input) {
    Map<Integer, INodeInstance> nodes = plan.getNodes();
    if (nodes == null) {
      throw new RuntimeException(String.format("%d Failed to set input for non-existing "
          + "existing sources: %s", workerID, plan.getNodeNames()));
    }

    for (Map.Entry<Integer, INodeInstance> e : nodes.entrySet()) {
      INodeInstance node = e.getValue();
      INode task = node.getNode();
      if (task instanceof Receptor && task instanceof ISource) {
        ((Receptor) task).add(inputKey, input);
      }
    }
  }

  /**
   * Extract output from a task graph
   *
   * @param graph the graph
   * @param plan plan created from the graph
   * @param taskName name of the output to retrieve
   * @return a DataObjectImpl with set of partitions from each task in this executor
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T> DataObject<T> getOutput(ComputeGraph graph, ExecutionPlan plan, String taskName) {
    Map<Integer, INodeInstance> nodes = plan.getNodes(taskName);
    if (nodes == null) {
      return new EmptyDataObject<>();
    }

    DataObject<T> dataSet = new DataObjectImpl<>(taskName, config);
    for (Map.Entry<Integer, INodeInstance> e : nodes.entrySet()) {
      INodeInstance node = e.getValue();
      INode task = node.getNode();
      if (task instanceof Collector) {
        DataPartition<T> partition = (DataPartition<T>) ((Collector) task).get();
        dataSet.addPartition(partition);
      } else {
        throw new RuntimeException("Cannot collect from node because it is not a collector: "
            + node);
      }
    }
    return dataSet;
  }

  /**
   * Extract output from a task graph
   *
   * @param graph the graph
   * @param plan plan created from the graph
   * @param dataName name of the data set
   * @return a DataObjectImpl with set of partitions from each task in this executor
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T> DataObject<T> getSinkOutput(ComputeGraph graph, ExecutionPlan plan,
                                         String dataName) {
    Map<Integer, INodeInstance> nodes = plan.getNodes();

    DataObject<T> dataSet = new DataObjectImpl<>(config);
    for (Map.Entry<Integer, INodeInstance> e : nodes.entrySet()) {
      INodeInstance node = e.getValue();
      INode task = node.getNode();
      if (task instanceof Collector && task instanceof ISink) {
        DataPartition partition = ((Collector) task).get(dataName);
        if (partition != null) {
          dataSet.addPartition(partition);
        } else {
          LOG.warning(String.format("Task id %d returned null for data %s",
              node.getId(), dataName));
        }
      }
    }
    return dataSet;
  }

  /**
   * Extract output from a task graph
   *
   * @param graph the graph
   * @param plan plan created from the graph
   * @param taskName name of the output to retrieve
   * @param dataName name of the data set
   * @return a DataObjectImpl with set of partitions from each task in this executor
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T> DataObject<T> getOutput(ComputeGraph graph, ExecutionPlan plan,
                                     String taskName, String dataName) {
    Map<Integer, INodeInstance> nodes = plan.getNodes(taskName);
    if (nodes == null) {
      return new EmptyDataObject<>();
    }

    DataObject<T> dataSet = new DataObjectImpl<T>(config);
    for (Map.Entry<Integer, INodeInstance> e : nodes.entrySet()) {
      INodeInstance node = e.getValue();
      INode task = node.getNode();
      if (task instanceof Collector) {
        DataPartition partition = ((Collector) task).get(dataName);
        if (partition != null) {
          dataSet.addPartition(partition);
        } else {
          LOG.warning(String.format("Task id %d returned null for data %s",
              node.getId(), dataName));
        }
      } else {
        throw new RuntimeException("Cannot collect from node because it is "
            + "not a collector: " + node);
      }
    }
    return dataSet;
  }

  private WorkerPlan createWorkerPlan() {
    List<Worker> workers = new ArrayList<>();
    for (JobMasterAPI.WorkerInfo workerInfo : workerInfoList) {
      Worker w = new Worker(workerInfo.getWorkerID());
      workers.add(w);
    }

    return new WorkerPlan(workers);
  }

  public void close() {
    if (executor != null) {
      executor.close();
    }
  }
}
