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
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.data.utils.KryoMemorySerializer;
import edu.iu.dsc.tws.executor.comm.IParallelOperation;
import edu.iu.dsc.tws.executor.comm.ParallelOperationFactory;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.task.api.INode;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.ITask;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Edge;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class DefaultExecutor implements IExecutor {
  private static final Logger LOG = Logger.getLogger(DefaultExecutor.class.getName());

  /**
   * no of threads used for this executor
   */
  private int noOfThreads;

  /**
   * Worker id
   */
  private int workerId;

  /**
   * Fixed thread executor
   */
  private FixedThreadExecutor executor;

  /**
   * Communications list
   */
  private Table<String, String, Communication> parOpTable = HashBasedTable.create();
  private Table<String, String, Communication> sendTable = HashBasedTable.create();
  private Table<String, String, Communication> recvTable = HashBasedTable.create();

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

  public DefaultExecutor(ResourcePlan plan) {
    this.workerId = plan.getThisId();
    this.executor = new FixedThreadExecutor();
    this.taskIdGenerator = new TaskIdGenerator();
    this.kryoMemorySerializer = new KryoMemorySerializer();
    this.resourcePlan = plan;
  }

  @Override
  public Execution schedule(Config cfg, DataFlowTaskGraph taskGraph,
                            TaskSchedulePlan taskSchedule) {

    // we need to build the task plan
    TaskPlan taskPlan = TaskPlanBuilder.build(resourcePlan, taskSchedule, taskIdGenerator);
    network = new TWSNetwork(cfg, taskPlan);
    ParallelOperationFactory opFactory = new ParallelOperationFactory(network);

    Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap = taskSchedule.getContainersMap();
    TaskSchedulePlan.ContainerPlan p = containersMap.get(workerId);
    if (p == null) {
      LOG.log(Level.INFO, "Cannot find worker in the task plan: " + workerId);
      return null;
    }

    Set<TaskSchedulePlan.TaskInstancePlan> instancePlan = p.getTaskInstances();
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
          Vertex child = taskGraph.childOfTask(v, e.getTaskEdge());
          // lets figure out the parents task id
          Set<Integer> srcTasks = taskIdGenerator.getTaskIds(v.getName(),
              ip.getTaskId(), taskGraph);
          Set<Integer> tarTasks = taskIdGenerator.getTaskIds(child.getName(),
              getTaskIdOfTask(child.getName(), taskSchedule), taskGraph);

          if (!parOpTable.contains(v.getName(), e.taskEdge)) {
            parOpTable.put(v.getName(), e.taskEdge, new Communication(v.getName(),
                e.taskEdge, srcTasks, tarTasks));
            sendTable.put(v.getName(), e.taskEdge, new Communication(v.getName(),
                e.taskEdge, srcTasks, tarTasks));
          }
        }
      }

      if (node instanceof ITask || node instanceof ISink) {
        // lets get the parent tasks
        Set<Edge> parentEdges = taskGraph.inEdges(v);
        for (Edge e : parentEdges) {
          Vertex parent = taskGraph.getParentOfTask(v, e.getTaskEdge());
          // lets figure out the parents task id
          Set<Integer> srcTasks = taskIdGenerator.getTaskIds(parent.getName(),
              getTaskIdOfTask(parent.getName(), taskSchedule), taskGraph);
          Set<Integer> tarTasks = taskIdGenerator.getTaskIds(v.getName(),
              ip.getTaskId(), taskGraph);

          if (!parOpTable.contains(parent.getName(), e.taskEdge)) {
            parOpTable.put(parent.getName(), e.taskEdge, new Communication(parent.getName(),
                e.taskEdge, srcTasks, tarTasks));
            recvTable.put(parent.getName(), e.taskEdge, new Communication(parent.getName(),
                e.taskEdge, srcTasks, tarTasks));
          }
        }
      }

      // lets create the instance
      createInstances(cfg, ip, v);
    }

    // now lets create the queues and start the execution
    Execution execution = new Execution();
    for (Table.Cell<String, String, Communication> cell : parOpTable.cellSet()) {
      Communication c = cell.getValue();

      // lets create the communication
      IParallelOperation op = opFactory.build(c.getName(), c.getSourceTasks(),
          c.getTargetTasks(), DataType.OBJECT);
      // now lets check the sources and targets that are in this executor

    }

    // lets start the execution

    return execution;
  }


  /**
   * Create an instance of a task,
   * @param cfg the configuration
   * @param ip instance plan
   * @param vertex vertex
   */
  private void createInstances(Config cfg, TaskSchedulePlan.TaskInstancePlan ip, Vertex vertex) {
    // lets add the task
    byte[] taskBytes = kryoMemorySerializer.serialize(vertex.getTask());
    INode newInstance = (INode) kryoMemorySerializer.deserialize(taskBytes);
    int taskId = taskIdGenerator.generateGlobalTaskId(vertex.getName(),
        ip.getTaskId(), ip.getTaskIndex());
    if (newInstance instanceof ITask) {
      taskInstances.put(vertex.getName(), taskId, new TaskInstance((ITask) newInstance,
          new ArrayBlockingQueue<>(1024), new ArrayBlockingQueue<>(1024), cfg));
    } else if (newInstance instanceof ISource) {
      sourceInstances.put(vertex.getName(), taskId, new SourceInstance((ISource) newInstance,
          new ArrayBlockingQueue<>(1024), cfg));
    } else if (newInstance instanceof ISink) {
      sinkInstances.put(vertex.getName(), taskId, new SinkInstance((ISink) newInstance,
          new ArrayBlockingQueue<>(1024), cfg));
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
    private String source;
    private String name;
    private Set<Integer> sourceTasks;
    private Set<Integer> targetTasks;

    Communication(String n, String src, Set<Integer> srcTasks, Set<Integer> tarTasks) {
      this.name = n;
      this.sourceTasks = srcTasks;
      this.source = src;
      this.targetTasks = tarTasks;
    }

    public String getName() {
      return name;
    }

    public Set<Integer> getSourceTasks() {
      return sourceTasks;
    }

    public String getSource() {
      return source;
    }

    public Set<Integer> getTargetTasks() {
      return targetTasks;
    }
  }

  @Override
  public void wait(Execution execution) {

  }

  @Override
  public void stop(Execution execution) {

  }
}
