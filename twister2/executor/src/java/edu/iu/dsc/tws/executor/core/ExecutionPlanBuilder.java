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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import edu.iu.dsc.tws.common.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.request.BlockingSendException;
import edu.iu.dsc.tws.comms.api.Communicator;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.IExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.api.IParallelOperation;
import edu.iu.dsc.tws.executor.core.batch.SinkBatchInstance;
import edu.iu.dsc.tws.executor.core.batch.SourceBatchInstance;
import edu.iu.dsc.tws.executor.core.batch.TaskBatchInstance;
import edu.iu.dsc.tws.executor.core.streaming.SinkStreamingInstance;
import edu.iu.dsc.tws.executor.core.streaming.SourceStreamingInstance;
import edu.iu.dsc.tws.executor.core.streaming.TaskStreamingInstance;
import edu.iu.dsc.tws.executor.core.streaming.window.SinkStreamingWindowingInstance;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.ftolerance.task.CheckpointableTask;
import edu.iu.dsc.tws.ftolerance.util.CheckpointingConfigurations;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.task.api.ICompute;
import edu.iu.dsc.tws.task.api.INode;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.schedule.ContainerPlan;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;
import edu.iu.dsc.tws.task.api.window.IWindowCompute;
import edu.iu.dsc.tws.task.api.window.api.IWindowedSink;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Edge;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class ExecutionPlanBuilder implements IExecutionPlanBuilder {
  private static final Logger LOG = Logger.getLogger(ExecutionPlanBuilder.class.getName());

  /**
   * Worker id
   */
  private int workerId;
  private CheckpointingClient checkpointingClient;

  /**
   * Communications list
   */
  private Table<String, String, Communication> parOpTable = HashBasedTable.create();

  /**
   * Creating separate tables for batch and streaming task instances
   **/
  private Table<String, Integer, TaskBatchInstance> batchTaskInstances
      = HashBasedTable.create();
  private Table<String, Integer, SourceBatchInstance> batchSourceInstances
      = HashBasedTable.create();
  private Table<String, Integer, SinkBatchInstance> batchSinkInstances
      = HashBasedTable.create();

  private Table<String, Integer, TaskStreamingInstance> streamingTaskInstances
      = HashBasedTable.create();
  private Table<String, Integer, SourceStreamingInstance> streamingSourceInstances
      = HashBasedTable.create();
  private Table<String, Integer, SinkStreamingInstance> streamingSinkInstances
      = HashBasedTable.create();

  private Table<String, Integer, SinkStreamingWindowingInstance> streamingSinkWindowingInstances
      = HashBasedTable.create();


  private Communicator network;

  private TaskIdGenerator taskIdGenerator;

  private EdgeGenerator edgeGenerator;

  private List<JobMasterAPI.WorkerInfo> workerInfoList;

  public ExecutionPlanBuilder(int workerID, List<JobMasterAPI.WorkerInfo> workerInfoList,
                              Communicator net, CheckpointingClient checkpointingClient) {
    this.workerId = workerID;
    this.checkpointingClient = checkpointingClient;
    this.taskIdGenerator = new TaskIdGenerator();
    this.workerInfoList = workerInfoList;
    this.edgeGenerator = new EdgeGenerator();
    this.network = net;
  }

  @Override
  public ExecutionPlan build(Config cfg, DataFlowTaskGraph taskGraph,
                             TaskSchedulePlan taskSchedule) {
    // we need to build the task plan
    TaskPlan taskPlan =
        TaskPlanBuilder.build(workerId, workerInfoList, taskSchedule, taskIdGenerator);
    ParallelOperationFactory opFactory = new ParallelOperationFactory(
        cfg, network, taskPlan, edgeGenerator);

    Map<Integer, ContainerPlan> containersMap = taskSchedule.getContainersMap();
    ContainerPlan conPlan = containersMap.get(workerId);
    if (conPlan == null) {
      LOG.log(Level.INFO, "Cannot find worker in the task plan: " + workerId);
      return null;
    }

    ExecutionPlan execution = new ExecutionPlan();

    Set<TaskInstancePlan> instancePlan = conPlan.getTaskInstances();

    long tasksVersion = 0L;

    if (CheckpointingConfigurations.isCheckpointingEnabled(cfg)) {
      Set<Integer> globalTasks = Collections.emptySet();
      if (workerId == 0) {
        globalTasks = containersMap.values().stream()
            .flatMap(containerPlan -> containerPlan.getTaskInstances().stream())
            .filter(ip -> taskGraph.vertex(ip.getTaskName())
                .getTask() instanceof CheckpointableTask)
            .map(ip -> taskIdGenerator.generateGlobalTaskId(ip.getTaskId(), ip.getTaskIndex()))
            .collect(Collectors.toSet());
      }

      try {
        Checkpoint.FamilyInitializeResponse familyInitializeResponse =
            this.checkpointingClient.initFamily(
                workerId,
                containersMap.size(),
                taskGraph.getGraphName(),
                globalTasks
            );

        tasksVersion = familyInitializeResponse.getVersion();
      } catch (BlockingSendException e) {
        throw new RuntimeException("Failed to register tasks with Checkpoint Manager", e);
      }

      LOG.info("Tasks will start with version " + tasksVersion);
    }

    // for each task we are going to create the communications
    for (TaskInstancePlan ip : instancePlan) {
      Vertex v = taskGraph.vertex(ip.getTaskName());
      Map<String, String> inEdges = new HashMap<>();
      Map<String, String> outEdges = new HashMap<>();
      if (v == null) {
        throw new RuntimeException("Non-existing task scheduled: " + ip.getTaskName());
      }

      INode node = v.getTask();
      if (node instanceof ICompute || node instanceof ISource || node instanceof IWindowCompute) {
        // lets get the communication
        Set<Edge> edges = taskGraph.outEdges(v);
        // now lets create the communication object
        for (Edge e : edges) {
          Vertex child = taskGraph.childOfTask(v, e.getName());
          // lets figure out the parents task id
          Set<Integer> srcTasks = taskIdGenerator.getTaskIds(v.getName(),
              ip.getTaskId(), taskGraph);
          Set<Integer> tarTasks = taskIdGenerator.getTaskIds(child.getName(),
              getTaskIdOfTask(child.getName(), taskSchedule), taskGraph);

          if (!parOpTable.contains(v.getName(), e.getName())) {
            parOpTable.put(v.getName(), e.getName(),
                new Communication(e, v.getName(), child.getName(), srcTasks, tarTasks));
          }
          outEdges.put(e.getName(), child.getName());
        }
      }

      if (node instanceof ICompute || node instanceof ISink || node instanceof IWindowedSink
          || node instanceof IWindowCompute) {
        // lets get the parent tasks
        Set<Edge> parentEdges = taskGraph.inEdges(v);
        for (Edge e : parentEdges) {
          Vertex parent = taskGraph.getParentOfTask(v, e.getName());
          // lets figure out the parents task id
          Set<Integer> srcTasks = taskIdGenerator.getTaskIds(parent.getName(),
              getTaskIdOfTask(parent.getName(), taskSchedule), taskGraph);
          Set<Integer> tarTasks = taskIdGenerator.getTaskIds(v.getName(),
              ip.getTaskId(), taskGraph);

          if (!parOpTable.contains(parent.getName(), e.getName())) {
            parOpTable.put(parent.getName(), e.getName(),
                new Communication(e, parent.getName(), v.getName(), srcTasks, tarTasks));
          }
          inEdges.put(e.getName(), parent.getName());
        }
      }

      // lets create the instance
      INodeInstance iNodeInstance = createInstances(cfg, taskGraph.getGraphName(),
          ip, v, taskGraph.getOperationMode(), inEdges, outEdges, taskSchedule, tasksVersion);
      // add to execution
      execution.addNodes(v.getName(), taskIdGenerator.generateGlobalTaskId(
          ip.getTaskId(), ip.getTaskIndex()), iNodeInstance);
    }

    // now lets create the queues and start the execution
    for (Table.Cell<String, String, Communication> cell : parOpTable.cellSet()) {
      Communication c = cell.getValue();

      // lets create the communication
      OperationMode operationMode = taskGraph.getOperationMode();
      IParallelOperation op = opFactory.build(c.getEdge(), c.getSourceTasks(), c.getTargetTasks(),
          operationMode);
      // now lets check the sources and targets that are in this executor
      Set<Integer> sourcesOfThisWorker = intersectionOfTasks(conPlan, c.getSourceTasks());
      Set<Integer> targetsOfThisWorker = intersectionOfTasks(conPlan, c.getTargetTasks());

      // set the parallel operation to the instance
      //let's separate the execution instance generation based on the Operation Mode
      // support to windows need to decide the type of instance that has to be initialized
      // so along with the operation mode, the windowing mode must be tested


      if (operationMode == OperationMode.STREAMING) {
        for (Integer i : sourcesOfThisWorker) {
          if (streamingTaskInstances.contains(c.getSourceTask(), i)) {
            TaskStreamingInstance taskStreamingInstance
                = streamingTaskInstances.get(c.getSourceTask(), i);
            taskStreamingInstance.registerOutParallelOperation(c.getEdge().getName(), op);
            op.registerSync(i, taskStreamingInstance);
          } else if (streamingSourceInstances.contains(c.getSourceTask(), i)) {
            SourceStreamingInstance sourceStreamingInstance
                = streamingSourceInstances.get(c.getSourceTask(), i);
            sourceStreamingInstance.registerOutParallelOperation(c.getEdge().getName(), op);
          } else {
            throw new RuntimeException("Not found: " + c.getSourceTask());
          }
        }

        for (Integer i : targetsOfThisWorker) {
          if (streamingTaskInstances.contains(c.getTargetTask(), i)) {
            TaskStreamingInstance taskStreamingInstance
                = streamingTaskInstances.get(c.getTargetTask(), i);
            op.register(i, taskStreamingInstance.getInQueue());
            taskStreamingInstance.registerInParallelOperation(c.getEdge().getName(), op);
            op.registerSync(i, taskStreamingInstance);
          } else if (streamingSinkInstances.contains(c.getTargetTask(), i)) {
            SinkStreamingInstance streamingSinkInstance
                = streamingSinkInstances.get(c.getTargetTask(), i);
            streamingSinkInstance.registerInParallelOperation(c.getEdge().getName(), op);
            op.register(i, streamingSinkInstance.getStreamingInQueue());
            op.registerSync(i, streamingSinkInstance);
          } else if (streamingSinkWindowingInstances.contains(c.getTargetTask(), i)) {
            SinkStreamingWindowingInstance sinkStreamingWindowingInstance
                = streamingSinkWindowingInstances.get(c.getTargetTask(), i);
            sinkStreamingWindowingInstance.registerInParallelOperation(c.getEdge().getName(), op);
            op.register(i, sinkStreamingWindowingInstance.getStreamingInQueue());
            op.registerSync(i, sinkStreamingWindowingInstance);
          } else {
            throw new RuntimeException("Not found: " + c.getTargetTask());
          }
        }
        execution.addOps(op);
      }

      if (operationMode == OperationMode.BATCH) {
        for (Integer i : sourcesOfThisWorker) {
          if (batchTaskInstances.contains(c.getSourceTask(), i)) {
            TaskBatchInstance taskBatchInstance = batchTaskInstances.get(c.getSourceTask(), i);
            taskBatchInstance.registerOutParallelOperation(c.getEdge().getName(), op);
          } else if (batchSourceInstances.contains(c.getSourceTask(), i)) {
            SourceBatchInstance sourceBatchInstance
                = batchSourceInstances.get(c.getSourceTask(), i);
            sourceBatchInstance.registerOutParallelOperation(c.getEdge().getName(), op);
          } else {
            throw new RuntimeException("Not found: " + c.getSourceTask());
          }
        }

        for (Integer i : targetsOfThisWorker) {
          if (batchTaskInstances.contains(c.getTargetTask(), i)) {
            TaskBatchInstance taskBatchInstance = batchTaskInstances.get(c.getTargetTask(), i);
            op.register(i, taskBatchInstance.getInQueue());
            taskBatchInstance.registerInParallelOperation(c.getEdge().getName(), op);
            op.registerSync(i, taskBatchInstance);
          } else if (batchSinkInstances.contains(c.getTargetTask(), i)) {
            SinkBatchInstance sinkBatchInstance = batchSinkInstances.get(c.getTargetTask(), i);
            sinkBatchInstance.registerInParallelOperation(c.getEdge().getName(), op);
            op.register(i, sinkBatchInstance.getBatchInQueue());
            op.registerSync(i, sinkBatchInstance);
          } else {
            throw new RuntimeException("Not found: " + c.getTargetTask());
          }
        }
        execution.addOps(op);
      }
    }
    return execution;
  }

  private Set<Integer> intersectionOfTasks(ContainerPlan cp,
                                           Set<Integer> tasks) {
    Set<Integer> cTasks = taskIdGenerator.getTaskIdsOfContainer(cp);
    cTasks.retainAll(tasks);
    return cTasks;
  }

  /**
   * Create an instance of a task,
   *
   * @param cfg the configuration
   * @param ip instance plan
   * @param vertex vertex
   */
  private INodeInstance createInstances(Config cfg,
                                        String taskGraphName,
                                        TaskInstancePlan ip,
                                        Vertex vertex, OperationMode operationMode,
                                        Map<String, String> inEdges,
                                        Map<String, String> outEdges,
                                        TaskSchedulePlan taskSchedule, long tasksVersion) {
    // lets add the task
    byte[] taskBytes = Utils.serialize(vertex.getTask());
    INode newInstance = (INode) Utils.deserialize(taskBytes);
    int taskId = taskIdGenerator.generateGlobalTaskId(ip.getTaskId(), ip.getTaskIndex());

    if (operationMode.equals(OperationMode.BATCH)) {
      if (newInstance instanceof ICompute) {
        if (newInstance instanceof ISink) {
          SinkBatchInstance v = new SinkBatchInstance((ICompute) newInstance,
              new LinkedBlockingQueue<>(), cfg, vertex.getName(), ip.getTaskId(),
              taskId, ip.getTaskIndex(), vertex.getParallelism(),
              workerId, vertex.getConfig().toMap(), inEdges, taskSchedule,
              this.checkpointingClient, taskGraphName, tasksVersion);
          batchSinkInstances.put(vertex.getName(), taskId, v);
          return v;
        } else {
          TaskBatchInstance v = new TaskBatchInstance((ICompute) newInstance,
              new LinkedBlockingQueue<>(),
              new LinkedBlockingQueue<>(), cfg,
              vertex.getName(), ip.getTaskId(), taskId, ip.getTaskIndex(),
              vertex.getParallelism(), workerId, vertex.getConfig().toMap(),
              inEdges, outEdges, taskSchedule, this.checkpointingClient,
              taskGraphName, tasksVersion);
          batchTaskInstances.put(vertex.getName(), taskId, v);
          return v;
        }
      } else if (newInstance instanceof ISource) {
        SourceBatchInstance v = new SourceBatchInstance((ISource) newInstance,
            new LinkedBlockingQueue<>(), cfg,
            vertex.getName(), ip.getTaskId(), taskId, ip.getTaskIndex(),
            vertex.getParallelism(), workerId, vertex.getConfig().toMap(), outEdges,
            taskSchedule, this.checkpointingClient, taskGraphName, tasksVersion);
        batchSourceInstances.put(vertex.getName(), taskId, v);
        return v;
      } else {
        throw new RuntimeException("Un-known type");
      }
    } else if (operationMode.equals(OperationMode.STREAMING)) {
      if (newInstance instanceof ICompute) {
        if (newInstance instanceof ISink) {
          SinkStreamingInstance v = new SinkStreamingInstance((ICompute) newInstance,
              new LinkedBlockingQueue<>(), cfg, vertex.getName(),
              ip.getTaskId(), taskId, ip.getTaskIndex(), vertex.getParallelism(), workerId,
              vertex.getConfig().toMap(), inEdges, taskSchedule,
              this.checkpointingClient, taskGraphName, tasksVersion);
          streamingSinkInstances.put(vertex.getName(), taskId, v);
          return v;
        } else {
          TaskStreamingInstance v = new TaskStreamingInstance((ICompute) newInstance,
              new LinkedBlockingQueue<>(),
              new LinkedBlockingQueue<>(), cfg,
              vertex.getName(), ip.getTaskId(), taskId, ip.getTaskIndex(),
              vertex.getParallelism(), workerId, vertex.getConfig().toMap(), inEdges,
              outEdges, taskSchedule, this.checkpointingClient, taskGraphName, tasksVersion);
          streamingTaskInstances.put(vertex.getName(), taskId, v);
          return v;
        }
      } else if (newInstance instanceof IWindowCompute) {
        if (newInstance instanceof IWindowedSink) {
          SinkStreamingWindowingInstance v =
              new SinkStreamingWindowingInstance((IWindowCompute) newInstance,
                  new LinkedBlockingQueue<>(), cfg, vertex.getName(),
                  ip.getTaskId(), taskId, ip.getTaskIndex(), vertex.getParallelism(), workerId,
                  vertex.getConfig().toMap(), inEdges, taskSchedule,
                  this.checkpointingClient, taskGraphName, tasksVersion);
          streamingSinkWindowingInstances.put(vertex.getName(), taskId, v);
          return v;
        } else {
          TaskStreamingInstance v = new TaskStreamingInstance((ICompute) newInstance,
              new LinkedBlockingQueue<>(),
              new LinkedBlockingQueue<>(), cfg,
              vertex.getName(), ip.getTaskId(), taskId, ip.getTaskIndex(),
              vertex.getParallelism(), workerId, vertex.getConfig().toMap(), inEdges,
              outEdges, taskSchedule, this.checkpointingClient, taskGraphName, tasksVersion);
          streamingTaskInstances.put(vertex.getName(), taskId, v);
          return v;
        }
      } else if (newInstance instanceof ISource) {
        SourceStreamingInstance v = new SourceStreamingInstance((ISource) newInstance,
            new LinkedBlockingQueue<>(), cfg,
            vertex.getName(), ip.getTaskId(), taskId, ip.getTaskIndex(),
            vertex.getParallelism(), workerId, vertex.getConfig().toMap(), outEdges,
            taskSchedule, this.checkpointingClient, taskGraphName, tasksVersion);
        streamingSourceInstances.put(vertex.getName(), taskId, v);
        return v;
      } else {
        throw new RuntimeException("Un-known type");
      }
    } else {
      // GRAPH Structures must be implemented here
      return null;
    }
  }


  private int getTaskIdOfTask(String name, TaskSchedulePlan plan) {
    for (ContainerPlan cp : plan.getContainers()) {
      for (TaskInstancePlan ip : cp.getTaskInstances()) {
        if (name.equals(ip.getTaskName())) {
          return ip.getTaskId();
        }
      }
    }
    throw new RuntimeException("Task without a schedule plan: " + name);
  }

  private class Communication {
    private Edge edge;
    private Set<Integer> sourceTasks;
    private Set<Integer> targetTasks;
    private String sourceTask;
    private String targetTask;

    Communication(Edge e, String srcTask, String tarTast,
                  Set<Integer> srcTasks, Set<Integer> tarTasks) {
      this.edge = e;
      this.targetTasks = tarTasks;
      this.sourceTasks = srcTasks;
      this.sourceTask = srcTask;
      this.targetTask = tarTast;
    }

    Set<Integer> getSourceTasks() {
      return sourceTasks;
    }

    Set<Integer> getTargetTasks() {
      return targetTasks;
    }

    public Edge getEdge() {
      return edge;
    }

    String getSourceTask() {
      return sourceTask;
    }

    String getTargetTask() {
      return targetTask;
    }
  }
}
