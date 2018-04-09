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
package edu.iu.dsc.tws.task.graph;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.task.api.ITask;

public class DataFlowTaskGraph extends BaseDataflowTaskGraph<ITask, TaskEdge> {
  private Map<String, ITask> taskMap = new HashMap<>();

  public DataFlowTaskGraph() {
    super(new VertexComparator(), new EdgeComparator());
  }

  @Override
  public boolean validate() {
    return super.validate();
  }

  @Override
  public void build() {
    // first validate
    validate();

    Set<ITask> ret = new HashSet<>();
    for (DirectedDataflowTaskEdge<ITask, TaskEdge> de : directedEdges) {
      taskMap.put(de.sourceTaskVertex.taskName(), de.sourceTaskVertex);
      taskMap.put(de.targetTaskVertex.taskName(), de.targetTaskVertex);
    }
  }

  @Override
  public TaskEdge createEdge(ITask sourceTaskVertex, ITask targetTaskVertex) {
    return super.createEdge(sourceTaskVertex, targetTaskVertex);
  }

  public boolean tasksEqual(ITask t1, ITask t2) {
    return t1.taskName().equals(t2.taskName());
  }

  public Set<TaskEdge> outEdges(ITask task) {
    return outgoingTaskEdgesOf(task);
  }

  public Set<TaskEdge> outEdges(String taskName) {
    ITask t = taskMap.get(taskName);
    if (t == null) {
      return new HashSet<>();
    }
    return outEdges(t);
  }

  public Set<TaskEdge> inEdges(ITask task) {
    return incomingTaskEdgesOf(task);
  }

  public Set<TaskEdge> inEdges(String taskName) {
    ITask t = taskMap.get(taskName);
    if (t == null) {
      return new HashSet<>();
    }
    return inEdges(t);
  }

  public Set<ITask> childrenOfTask(String taskName) {
    ITask t = taskMap.get(taskName);
    if (t == null) {
      return new HashSet<>();
    }
    return childrenOfTask(t);
  }

  public Set<ITask> childrenOfTask(ITask t) {
    return childrenOfTask(t.taskName());
  }

  public ITask childOfTask(ITask task, String edge) {
    Set<TaskEdge> edges = outEdges(task);

    TaskEdge taskEdge = null;
    for (TaskEdge e : edges) {
      if (e.taskEdge.equals(edge)) {
        taskEdge = e;
      }
    }

    if (taskEdge != null) {
      return connectedChildTask(task, taskEdge);
    } else {
      return null;
    }
  }

  public ITask getParentOfTask(ITask task, String edge) {
    Set<TaskEdge> edges = inEdges(task);

    TaskEdge taskEdge = null;
    for (TaskEdge e : edges) {
      if (e.taskEdge.equals(edge)) {
        taskEdge = e;
      }
    }

    if (taskEdge != null) {
      return connectedChildTask(task, taskEdge);
    } else {
      return null;
    }
  }

  private static class VertexComparator implements Comparator<ITask> {
    @Override
    public int compare(ITask o1, ITask o2) {
      return new StringComparator().compare(o1.taskName(), o2.taskName());

    }
  }

  private static class EdgeComparator implements Comparator<TaskEdge> {
    @Override
    public int compare(TaskEdge o1, TaskEdge o2) {
      return new StringComparator().compare(o1.taskEdge, o2.taskEdge);
    }
  }

  public static class StringComparator implements Comparator<String> {
    public int compare(String obj1, String obj2) {
      if (obj1 == null) {
        return -1;
      }
      if (obj2 == null) {
        return 1;
      }
      if (obj1.equals(obj2)) {
        return 0;
      }
      return obj1.compareTo(obj2);
    }
  }
}
