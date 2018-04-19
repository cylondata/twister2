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

  private EdgeGenerator edgeGenerator;

  public DefaultExecutor(ResourcePlan plan) {
    this.workerId = plan.getThisId();
    this.taskIdGenerator = new TaskIdGenerator();
    this.kryoMemorySerializer = new KryoMemorySerializer();
    this.resourcePlan = plan;
    this.edgeGenerator = new EdgeGenerator();
  }

  @Override
  public Execution schedule(Config cfg, DataFlowTaskGraph taskGraph,
                            TaskSchedulePlan taskSchedule) {

    // we need to build the task plan
    TaskPlan taskPlan = TaskPlanBuilder.build(resourcePlan, taskSchedule, taskIdGenerator);
    network = new TWSNetwork(cfg, taskPlan);
    ParallelOperationFactory opFactory = new ParallelOperationFactory(
        cfg, network.getChannel(), taskPlan, edgeGenerator);

    Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap = taskSchedule.getContainersMap();
    TaskSchedulePlan.ContainerPlan conPlan = containersMap.get(workerId);
    if (conPlan == null) {
      LOG.log(Level.INFO, "Cannot find worker in the task plan: " + workerId);
      return null;
    }

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

          if (!parOpTable.contains(v.getName(), e.name)) {
            parOpTable.put(v.getName(), e.name,
                new Communication(e.getName(), e.getOperation(), srcTasks, tarTasks));
            sendTable.put(v.getName(), e.name,
                new Communication(e.getName(), e.getOperation(), srcTasks, tarTasks));
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

          if (!parOpTable.contains(parent.getName(), e.name)) {
            parOpTable.put(parent.getName(), e.name,
                new Communication(e.getName(), e.getOperation(), srcTasks, tarTasks));
            recvTable.put(parent.getName(), e.name,
                new Communication(e.getName(), e.getOperation(), srcTasks, tarTasks));
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
      IParallelOperation op = opFactory.build(c.getOperation(), c.getSourceTasks(),
          c.getTargetTasks(), DataType.OBJECT, c.getName());
      // now lets check the sources and targets that are in this executor
      Set<Integer> sourcesOfThisWorker = intersectionOfTasks(conPlan, c.getSourceTasks());
      Set<Integer> targetsOfThisWorker = intersectionOfTasks(conPlan, c.getTargetTasks());

      // todo
      // set the parallel operation to the instance
      // lets see weather this comunication belongs to a task instance
      for (Integer i : sourcesOfThisWorker) {
        if (taskInstances.contains(c.getSourceTasks(), i)) {
          taskInstances.get(c.getSourceTasks(), i).registerOutParallelOperation(c.getName(), op);
        } else if (sourceInstances.contains(c.getSourceTasks(), i)) {
          sourceInstances.get(c.getSourceTasks(), i).registerOutParallelOperation(c.getName(), op);
        } else {
          throw new RuntimeException("Not found");
        }
      }

      for (Integer i : targetsOfThisWorker) {
        if (taskInstances.contains(c.getSourceTasks(), i)) {
          taskInstances.get(c.getSourceTasks(), i).getInQueue();
        } else if (sourceInstances.contains(c.getSourceTasks(), i)) {
          sourceInstances.get(c.getSourceTasks(), i).registerOutParallelOperation(c.getName(), op);
        } else {
          throw new RuntimeException("Not found");
        }
      }
    }

    // lets start the execution
    ThreadSharingExecutor threadSharingExecutor =
        new ThreadSharingExecutor(noOfThreads, network.getChannel());
    threadSharingExecutor.execute(execution);

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
  private void createInstances(Config cfg, TaskSchedulePlan.TaskInstancePlan ip, Vertex vertex) {
    // lets add the task
    byte[] taskBytes = kryoMemorySerializer.serialize(vertex.getTask());
    INode newInstance = (INode) kryoMemorySerializer.deserialize(taskBytes);
    int taskId = taskIdGenerator.generateGlobalTaskId(vertex.getName(),
        ip.getTaskId(), ip.getTaskIndex());
    if (newInstance instanceof ITask) {
      taskInstances.put(vertex.getName(), taskId, new TaskInstance((ITask) newInstance,
          new ArrayBlockingQueue<>(1024),
          new ArrayBlockingQueue<>(1024), cfg, edgeGenerator));
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
    private String name;
    private Set<Integer> sourceTasks;
    private Set<Integer> targetTasks;
    private String operation;

    Communication(String tarTask, String op,
                  Set<Integer> srcTasks, Set<Integer> tarTasks) {
      this.sourceTasks = srcTasks;
      this.name = tarTask;
      this.targetTasks = tarTasks;
      this.operation = op;
    }

    public Set<Integer> getSourceTasks() {
      return sourceTasks;
    }

    public String getName() {
      return name;
    }

    public Set<Integer> getTargetTasks() {
      return targetTasks;
    }

    public String getOperation() {
      return operation;
    }
  }

  @Override
  public void wait(Execution execution) {

  }

  @Override
  public void stop(Execution execution) {

  }
}
