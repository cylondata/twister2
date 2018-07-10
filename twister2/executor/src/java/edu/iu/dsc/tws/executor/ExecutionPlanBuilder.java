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
package edu.iu.dsc.tws.executor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;
import edu.iu.dsc.tws.executor.comm.IParallelOperation;
import edu.iu.dsc.tws.executor.comm.ParallelOperationFactory;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.task.api.INode;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.ITask;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Edge;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class ExecutionPlanBuilder implements IExecutor {
  private static final Logger LOG = Logger.getLogger(ExecutionPlanBuilder.class.getName());

  /**
   * no of threads used for this executor
   */
  private int noOfThreads;

  /**
   * Worker id
   */
  private int workerId;

  /**
   * Communications list
   */
  private Table<String, String, Communication> parOpTable = HashBasedTable.create();

  /**
   * For each task we have multiple instances
   */
  private Table<String, Integer, TaskInstance> taskInstances = HashBasedTable.create();
  private Table<String, Integer, SourceInstance> sourceInstances = HashBasedTable.create();
  private Table<String, Integer, SinkInstance> sinkInstances = HashBasedTable.create();

  private TWSNetwork network;

  private ResourcePlan resourcePlan;

  private TaskIdGenerator taskIdGenerator;

  private KryoMemorySerializer kryoMemorySerializer;

  private EdgeGenerator edgeGenerator;

  public ExecutionPlanBuilder(ResourcePlan plan, TWSNetwork net) {
    this.workerId = plan.getThisId();
    this.taskIdGenerator = new TaskIdGenerator();
    this.kryoMemorySerializer = new KryoMemorySerializer();
    this.resourcePlan = plan;
    this.edgeGenerator = new EdgeGenerator();
    this.network = net;
  }

  @Override
  public ExecutionPlan schedule(Config cfg, DataFlowTaskGraph taskGraph,
                                TaskSchedulePlan taskSchedule) {

    noOfThreads = ExecutorContext.threadsPerContainer(cfg);
    LOG.log(Level.INFO, " ExecutionBuilder Thread Count : " + noOfThreads);

    // we need to build the task plan
    TaskPlan taskPlan = TaskPlanBuilder.build(resourcePlan, taskSchedule, taskIdGenerator);
    ParallelOperationFactory opFactory = new ParallelOperationFactory(
        cfg, network.getChannel(), taskPlan, edgeGenerator);

    Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap = taskSchedule.getContainersMap();
    TaskSchedulePlan.ContainerPlan conPlan = containersMap.get(workerId);
    if (conPlan == null) {
      LOG.log(Level.INFO, "Cannot find worker in the task plan: " + workerId);
      return null;
    }

    ExecutionPlan execution = new ExecutionPlan();

    Set<TaskSchedulePlan.TaskInstancePlan> instancePlan = conPlan.getTaskInstances();
    // for each task we are going to create the communications
    for (TaskSchedulePlan.TaskInstancePlan ip : instancePlan) {
      Vertex v = taskGraph.vertex(ip.getTaskName());

      if (v == null) {
        throw new RuntimeException("Non-existing task scheduled: " + ip.getTaskName());
      }

      INode node = v.getTask();
      if (node instanceof ITask || node instanceof ISource) {
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
        }
      }

      if (node instanceof ITask || node instanceof ISink) {
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
        }
      }

      // lets create the instance
      INodeInstance iNodeInstance = createInstances(cfg, ip, v);
      execution.addNodes(taskIdGenerator.generateGlobalTaskId(
          v.getName(), ip.getTaskId(), ip.getTaskIndex()), iNodeInstance);
    }

    // now lets create the queues and start the execution
    for (Table.Cell<String, String, Communication> cell : parOpTable.cellSet()) {
      Communication c = cell.getValue();

      // lets create the communication
      IParallelOperation op = opFactory.build(c.getEdge(), c.getSourceTasks(), c.getTargetTasks());
      // now lets check the sources and targets that are in this executor
      Set<Integer> sourcesOfThisWorker = intersectionOfTasks(conPlan, c.getSourceTasks());
      Set<Integer> targetsOfThisWorker = intersectionOfTasks(conPlan, c.getTargetTasks());

      // set the parallel operation to the instance
      // lets see weather this comunication belongs to a task instance
      for (Integer i : sourcesOfThisWorker) {
        if (taskInstances.contains(c.getSourceTask(), i)) {
          LOG.info("SourceofThisWorker TaskInstance");
          TaskInstance taskInstance = taskInstances.get(c.getSourceTask(), i);
          taskInstance.registerOutParallelOperation(c.getEdge().getName(), op);
        } else if (sourceInstances.contains(c.getSourceTask(), i)) {
          SourceInstance sourceInstance = sourceInstances.get(c.getSourceTask(), i);
          LOG.info("Edge : " + c.getEdge().getName() + ", Op : " + op.getClass().getName());
          sourceInstance.registerOutParallelOperation(c.getEdge().getName(), op);
        } else {
          throw new RuntimeException("Not found: " + c.getSourceTask());
        }
      }

      for (Integer i : targetsOfThisWorker) {
        if (taskInstances.contains(c.getTargetTask(), i)) {
          LOG.info("TargetofThisWorker TaskInstance");
          TaskInstance taskInstance = taskInstances.get(c.getTargetTask(), i);
          op.register(i, taskInstance.getInQueue());
          taskInstance.registerInParallelOperation(c.getEdge().getName(), op);
        } else if (sinkInstances.contains(c.getTargetTask(), i)) {
          SinkInstance sourceInstance = sinkInstances.get(c.getTargetTask(), i);
          sourceInstance.registerInParallelOperation(c.getEdge().getName(), op);
          op.register(i, sourceInstance.getInQueue());
        } else {
          throw new RuntimeException("Not found: " + c.getTargetTask());
        }
      }
      execution.addOps(op);
    }

    return execution;
  }

  private Set<Integer> intersectionOfTasks(TaskSchedulePlan.ContainerPlan cp,
                                                Set<Integer> tasks) {
    Set<Integer> cTasks = taskIdGenerator.getTaskIdsOfContainer(cp);
    cTasks.retainAll(tasks);
    return cTasks;
  }

  /**
   * Create an instance of a task,
   * @param cfg the configuration
   * @param ip instance plan
   * @param vertex vertex
   */
  private INodeInstance createInstances(Config cfg,
                                        TaskSchedulePlan.TaskInstancePlan ip, Vertex vertex) {
    // lets add the task
    byte[] taskBytes = kryoMemorySerializer.serialize(vertex.getTask());
    INode newInstance = (INode) kryoMemorySerializer.deserialize(taskBytes);
    int taskId = taskIdGenerator.generateGlobalTaskId(vertex.getName(),
        ip.getTaskId(), ip.getTaskIndex());
    if (newInstance instanceof ITask) {
      TaskInstance v = new TaskInstance((ITask) newInstance,
          new ArrayBlockingQueue<>(1024),
          new ArrayBlockingQueue<>(1024), cfg, edgeGenerator,
          vertex.getName(), taskId, ip.getTaskIndex(),
          vertex.getParallelism(), workerId, vertex.getConfig().toMap());
      taskInstances.put(vertex.getName(), taskId, v);
      return v;
    } else if (newInstance instanceof ISource) {
      SourceInstance v = new SourceInstance((ISource) newInstance,
          new ArrayBlockingQueue<>(1024), cfg,
          vertex.getName(), taskId, ip.getTaskIndex(),
          vertex.getParallelism(), workerId, vertex.getConfig().toMap());
      sourceInstances.put(vertex.getName(), taskId, v);
      return v;
    } else if (newInstance instanceof ISink) {
      SinkInstance v = new SinkInstance((ISink) newInstance,
          new ArrayBlockingQueue<>(1024), cfg, taskId, ip.getTaskIndex(),
          vertex.getParallelism(), workerId, vertex.getConfig().toMap());
      sinkInstances.put(vertex.getName(), taskId, v);
      return v;
    } else {
      throw new RuntimeException("Un-known type");
    }
  }


  private int getTaskIdOfTask(String name, TaskSchedulePlan plan) {
    for (TaskSchedulePlan.ContainerPlan cp : plan.getContainers()) {
      for (TaskSchedulePlan.TaskInstancePlan ip : cp.getTaskInstances()) {
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

    public Set<Integer> getSourceTasks() {
      return sourceTasks;
    }

    public Set<Integer> getTargetTasks() {
      return targetTasks;
    }

    public Edge getEdge() {
      return edge;
    }

    public String getSourceTask() {
      return sourceTask;
    }

    public String getTargetTask() {
      return targetTask;
    }
  }

  @Override
  public void wait(ExecutionPlan execution) {

  }

  @Override
  public void stop(ExecutionPlan execution) {

  }
}
