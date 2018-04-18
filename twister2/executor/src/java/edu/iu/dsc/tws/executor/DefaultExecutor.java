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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
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
  private Table<String, String, Communication> sendingTable = HashBasedTable.create();
  private Table<String, String, Communication> recvTable = HashBasedTable.create();

  /**
   * For each task we have multiple instances
   */
  private Table<String, Integer, TaskInstance> taskInstances = HashBasedTable.create();
  private Table<String, Integer, SourceInstance> sourceInstances = HashBasedTable.create();
  private Table<String, Integer, TaskInstance> sinkInstances = HashBasedTable.create();

  private TWSNetwork network;

  private ResourcePlan resourcePlan;

  private TaskIdGenerator taskIdGenerator;

  public DefaultExecutor(int workerId) {
    this.workerId = workerId;
    this.executor = new FixedThreadExecutor();
  }

  @Override
  public Execution schedule(Config cfg, DataFlowTaskGraph taskGraph,
                            TaskSchedulePlan taskSchedule) {
    Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap = taskSchedule.getContainersMap();
    TaskSchedulePlan.ContainerPlan p = containersMap.get(workerId);
    if (p == null) {
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
          HashSet<Integer> srcTasks = new HashSet<>();
          HashSet<Integer> tarTasks = new HashSet<>();
          if (!sendingTable.contains(v.getName(), e.taskEdge)) {
            sendingTable.put(v.getName(), e.taskEdge, new Communication(v.getName(),
                e.taskEdge, srcTasks, tarTasks));
          }
        }
      }

      if (node instanceof ITask || node instanceof ISink) {
        // lets get the parent tasks
        Set<Edge> parentEdges = taskGraph.inEdges(v);
        for (Edge e : parentEdges) {
          Vertex parent = taskGraph.getParentOfTask(v, e.getTaskEdge());
          HashSet<Integer> srcTasks = new HashSet<>();
          HashSet<Integer> tarTasks = new HashSet<>();
          if (!sendingTable.contains(parent.getName(), e.taskEdge)) {
            recvTable.put(parent.getName(), e.taskEdge, new Communication(parent.getName(),
                e.taskEdge, srcTasks, tarTasks));
          }
        }
      }
    }

    // we need to build the task plan
    TaskPlan taskPlan = TaskPlanBuilder.build(resourcePlan, taskSchedule, taskIdGenerator);

    // now lets create the queues and start the execution
    Execution execution = new Execution();
//    for (Table.Cell<String, String, Communication> c : sendingTable.cellSet()) {
//
//    }

    return execution;
  }

  private class Communication {
    private String source;
    private String name;
    private Set<Integer> sourceTasks = new HashSet<>();
    private Set<Integer> targetTasks = new HashSet<>();

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
  }

  @Override
  public void wait(Execution execution) {

  }

  @Override
  public void stop(Execution execution) {

  }
}
