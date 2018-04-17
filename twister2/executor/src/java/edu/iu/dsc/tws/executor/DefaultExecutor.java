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

  public DefaultExecutor(int workerId) {
    this.workerId = workerId;
    this.executor = new FixedThreadExecutor();
  }

  @Override
  public Execution schedule(DataFlowTaskGraph taskGraph,
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
          HashSet<Integer> tasks = new HashSet<>();
          if (!sendingTable.contains(v.getName(), e.taskEdge)) {
            sendingTable.put(v.getName(), e.taskEdge, new Communication(v.getName(),
                e.taskEdge, tasks));
          }
        }
      }

      if (node instanceof ITask || node instanceof ISink) {
        // lets get the parent tasks
        Set<Edge> parentEdges = taskGraph.inEdges(v);
        for (Edge e : parentEdges) {
          Vertex parent = taskGraph.getParentOfTask(v, e.getTaskEdge());
          HashSet<Integer> tasks = new HashSet<>();
          if (!sendingTable.contains(parent.getName(), e.taskEdge)) {
            recvTable.put(parent.getName(), e.taskEdge, new Communication(parent.getName(),
                e.taskEdge, tasks));
          }
        }
      }
    }

    // now lets create the queues and start the execution
    Execution execution = new Execution();


    return null;
  }

  private class Communication {
    private String source;
    private String name;
    private Set<Integer> tasks = new HashSet<>();

    Communication(String n, String src, Set<Integer> tasks) {
      this.name = n;
      this.tasks = tasks;
      this.source = src;
    }

    public String getName() {
      return name;
    }

    public Set<Integer> getTasks() {
      return tasks;
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
