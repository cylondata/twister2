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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.executor.IExecutor;
import edu.iu.dsc.tws.api.compute.executor.INodeInstance;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.compute.nodes.ISource;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskSchedulePlan;
import edu.iu.dsc.tws.api.compute.schedule.elements.Worker;
import edu.iu.dsc.tws.api.compute.schedule.elements.WorkerPlan;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.dataset.EmptyDataObject;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.faulttolerance.Fault;
import edu.iu.dsc.tws.api.faulttolerance.FaultAcceptable;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.executor.core.ExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.threading.ExecutorFactory;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.tsched.taskscheduler.TaskScheduler;

/**
 * The task executor API, this class can be used to create an execution plan and execute
 */
public class TaskExecutor implements FaultAcceptable {
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

  /**
   * Checkpointing client
   */
  private CheckpointingClient checkpointingClient;

  /**
   * The executor used by this task executor
   */
  private ExecutorFactory executor;

  /**
   * Keep the data objects with name -> object
   */
  private Map<String, DataObject> dataObjectMap = new HashMap<>();

  /**
   * Keep the current executors
   */
  private ExecutorList currentExecutors;

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
    this.executor = new ExecutorFactory(config, workerID, communicator.getChannel());
    this.currentExecutors = new ExecutorList();
  }

  public TaskExecutor(WorkerEnvironment workerEnv) {
    this.config = workerEnv.getConfig();
    this.workerID = workerEnv.getWorkerId();
    this.workerInfoList = workerEnv.getWorkerList();
    this.communicator = workerEnv.getCommunicator();
    this.checkpointingClient = workerEnv.getWorkerController().getCheckpointingClient();
    this.executor = new ExecutorFactory(config, workerID, communicator.getChannel());
    this.currentExecutors = new ExecutorList();
  }

  /**
   * Create an execution plan from the given graph
   *
   * @param graph task graph
   * @return the data set
   */
  public ExecutionPlan plan(ComputeGraph graph) {

    TaskScheduler taskScheduler = new TaskScheduler();
    taskScheduler.initialize(config);

    WorkerPlan workerPlan = createWorkerPlan();
    TaskSchedulePlan taskSchedulePlan = taskScheduler.schedule(graph, workerPlan);

    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(
        workerID, workerInfoList, communicator, this.checkpointingClient);
    return executionPlanBuilder.build(config, graph, taskSchedulePlan);
  }

  /**
   * Create execution plans for each graph.
   *
   * @param graph list of graphs
   * @return graph name {@literal ->} execution plan map
   */
  public Map<String, ExecutionPlan> plan(ComputeGraph... graph) {

    WorkerPlan workerPlan = createWorkerPlan();

    TaskScheduler taskScheduler = new TaskScheduler();
    taskScheduler.initialize(config);
    Map<String, TaskSchedulePlan> schedulePlanMap = taskScheduler.schedule(workerPlan, graph);

    Map<String, ExecutionPlan> executionPlanMap = new LinkedHashMap<>();
    for (ComputeGraph aGraph : graph) {
      TaskSchedulePlan taskSchedulePlan = schedulePlanMap.get(aGraph.getGraphName());
      ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(
          workerID, workerInfoList, communicator, this.checkpointingClient);
      ExecutionPlan executionPlan = executionPlanBuilder.build(config, aGraph, taskSchedulePlan);
      executionPlanMap.put(aGraph.getGraphName(), executionPlan);
    }
    return executionPlanMap;
  }

  /**
   * Create a execution plan
   *
   * @param graph the graph
   * @param taskSchedulePlan task schedule
   * @return the execution plan
   */
  public ExecutionPlan executionPlan(ComputeGraph graph, TaskSchedulePlan taskSchedulePlan) {
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

    IExecutor ex = executor.getExecutor(newCfg, plan, graph.getOperationMode(),
        new ExecutionHookImpl(config, dataObjectMap, plan, currentExecutors));
    ex.execute();
    ex.closeExecution();
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
    execute(config, graph, plan);
  }

  /**
   * Execute a plan and a graph. This call blocks until the execution finishes. In case of
   * streaming, this call doesn't return while for batch computations it returns after
   * the execution is done.
   *
   * @param graph the dataflow graph
   */
  public void execute(ComputeGraph graph) {
    ExecutionPlan plan = plan(graph);
    execute(config, graph, plan);
  }


  /**
   * Execute a plan and a graph. This call blocks until the execution finishes. In case of
   * streaming, this call doesn't return while for batch computations it returns after
   * the execution is done.
   *
   * @param graph the dataflow graph
   * @param plan the execution plan
   */
  public IExecutor createExecution(ComputeGraph graph, ExecutionPlan plan) {
    IExecutor ex = executor.getExecutor(config, plan, graph.getOperationMode(),
        new ExecutionHookImpl(config, dataObjectMap, plan, currentExecutors));
    currentExecutors.add(ex);
    return ex;
  }

  /**
   * Execute a plan and a graph. This call blocks until the execution finishes. In case of
   * streaming, this call doesn't return while for batch computations it returns after
   * the execution is done.
   *
   * @param graph the dataflow graph
   */
  public IExecutor createExecution(ComputeGraph graph) {
    ExecutionPlan plan = plan(graph);
    IExecutor ex = executor.getExecutor(config, plan, graph.getOperationMode(),
        new ExecutionHookImpl(config, dataObjectMap, plan, currentExecutors));
    currentExecutors.add(ex);
    return ex;
  }

  /**
   * Add input to the the task instances
   *
   * @param graph task graph
   * @param plan execution plan
   * @param taskName task name
   * @param inputKey inputkey
   * @param input input
   * @deprecated Inputs are automatically handled now
   */
  @Deprecated
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
   * @param ex execution plan
   * @param taskName task name
   * @param inputKey inputkey
   * @param input input
   * @deprecated Inputs are automatically handled now
   */
  @Deprecated
  public void addInput(IExecutor ex,
                       String taskName, String inputKey, DataObject<?> input) {
    addInput(null, ex.getExecutionPlan(), taskName, inputKey, input);
  }

  /**
   * Add input to the the task instances
   *
   * @param graph task graph
   * @param plan execution plan
   * @param inputKey inputkey
   * @param input input
   * @deprecated Inputs are handled automatically now
   */
  @Deprecated
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
   * Add input to the the task instances
   *
   * @param ex execution plan
   * @param inputKey inputkey
   * @param input input
   * @deprecated Inputs are handled automatically now
   */
  @Deprecated
  public void addSourceInput(IExecutor ex,
                             String inputKey, DataObject<Object> input) {
    addSourceInput(null, ex.getExecutionPlan(), inputKey, input);
  }

  /**
   * Extract output from a task graph
   *
   * @param graph the graph
   * @param plan plan created from the graph
   * @param taskName name of the output to retrieve
   * @return a DataObjectImpl with set of partitions from each task in this executor
   * @deprecated There is no need of using this method anymore.
   * Input - Output is handled automatically
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Deprecated
  public <T> DataObject<T> getOutput(ComputeGraph graph, ExecutionPlan plan, String taskName) {
    // todo: fix this! this functionality is broken
    return EmptyDataObject.getInstance();
  }

  /**
   * Extract output from a task graph
   *
   * @param graph the graph
   * @param plan plan created from the graph
   * @param taskName name of the output to retrieve
   * @param dataName name of the data set
   * @return a DataObjectImpl with set of partitions from each task in this executor
   * @deprecated There is no need of using this method anymore.
   * Input - Output is handled automatically
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Deprecated
  public <T> DataObject<T> getOutput(ComputeGraph graph, ExecutionPlan plan,
                                     String taskName, String dataName) {
    return this.dataObjectMap.getOrDefault(dataName, EmptyDataObject.getInstance());
  }

  public <T> DataObject<T> getOutput(String varName) {
    return this.dataObjectMap.get(varName);
  }

  public boolean isOutputAvailable(String name) {
    return this.dataObjectMap.containsKey(name);
  }

  public void addInput(String name, DataObject dataObject) {
    this.dataObjectMap.put(name, dataObject);
  }

  /**
   * This method collects all the output from the provided {@link ExecutionPlan}.
   * The partition IDs will be assigned just before adding the partitions to the {@link DataObject}
   */
  public static void collectData(Config cfg, ExecutionPlan executionPlan,
                                 Map<String, DataObject> dataMap) {
    Map<Integer, INodeInstance> nodes = executionPlan.getNodes();
    Map<String, DataObject> dataObjectMapForPlan = new HashMap<>();
    if (nodes != null) {
      nodes.forEach((taskId, node) -> {
        INode task = node.getNode();
        if (task instanceof Collector) {
          Set<String> collectibleNames = ((Collector) task).getCollectibleNames();
          collectibleNames.forEach(name -> {
            DataPartition partition = ((Collector) task).get(name);

            // if this task outs only one partition and user has implemented no arg get() method
            if (collectibleNames.size() == 1 && partition == null) {
              partition = ((Collector) task).get();
            }

            if (partition != null) {
              partition.setId(node.getIndex());
              dataObjectMapForPlan.computeIfAbsent(name,
                  n -> new DataObjectImpl<>(cfg)).addPartition(partition);
            } else {
              LOG.warning(String.format("Task index %d  of task %d returned null for data %s",
                  node.getIndex(), node.getId(), name));
            }
          });
        }
      });
    }
    dataMap.putAll(dataObjectMapForPlan);
  }

  /**
   * This method distributes collected {@link DataPartition}s to the
   * intended {@link Receptor}s
   */
  public static void distributeData(ExecutionPlan executionPlan, Map<String, DataObject> dataMap) {
    Map<Integer, INodeInstance> nodes = executionPlan.getNodes();
    if (nodes != null) {
      nodes.forEach((id, node) -> {
        INode task = node.getNode();
        if (task instanceof Receptor) {
          Set<String> receivableNames = ((Receptor) task).getReceivableNames();
          for (String receivableName : receivableNames) {
            DataObject dataObject = dataMap.get(receivableName);
            if (dataObject == null) {
              throw new Twister2RuntimeException("Couldn't find input data" + receivableName
                  + " for task " + node.getId());
            }
            DataPartition partition = dataObject.getPartition(node.getIndex());
            if (partition == null) {
              throw new Twister2RuntimeException("Couldn't find input data" + receivableName
                  + " for task index " + node.getIndex() + " of task" + node.getId());
            }
            ((Receptor) task).add(receivableName, dataObject);
            ((Receptor) task).add(receivableName, partition);
          }
        }
      });
    }
  }

  /**
   * This method can be used to clear {@link DataPartition}s collected from previous
   * task graphs, which are no longer required
   */
  public void clearData(String var) {
    DataObject dataObject = this.dataObjectMap.remove(var);
    if (dataObject != null) {
      for (DataPartition partition : dataObject.getPartitions()) {
        // in memory partitions will do nothing. Disk backed partitions will clear the files
        partition.clear();
      }
    }
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
  }

  @Override
  public void onFault(Fault fault) {
    currentExecutors.onFault(fault);
  }

}
