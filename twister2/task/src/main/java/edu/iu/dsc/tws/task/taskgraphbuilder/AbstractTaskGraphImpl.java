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
package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public abstract class AbstractTaskGraphImpl<TV, TE> extends AbstractTaskGraph<TV, TE>
    implements TaskGraph<TV, TE>, Cloneable, Serializable {

  private static final long serialVersionUID = 223333333344444666L;

  private TaskEdgeFactory<TV, TE> taskEdgeFactory;
  private TaskGraphSpecifics taskGraphSpecifics;
  private Map<TE, IntrusiveTaskEdge> taskEdgeMap;
  private TaskEdgeSetFactory<TV, TE> taskEdgeSetFactory;
  //private TypeUtil<TV> vertexTypeDecl = null;
  private TypeUtil<TV> vertexTypeDecl = new TypeUtil<>();


  public AbstractTaskGraphImpl(TaskEdgeFactory<TV, TE> taskEdgeFactory) {
    if (taskEdgeFactory == null) {
      throw new NullPointerException();
    }
    this.taskEdgeFactory = taskEdgeFactory;
    this.taskEdgeMap = new LinkedHashMap<>();
    this.taskEdgeSetFactory = new ArrayListFactory<TV, TE>();
    this.taskGraphSpecifics = createTaskGraphSpecifics();
  }

  @Override
  public TaskEdgeFactory<TV, TE> getTaskEdgeFactory() {
    return taskEdgeFactory;
  }

  public void setTaskEdgeFactory(TaskEdgeFactory<TV, TE> taskEdgeFactory) {
    this.taskEdgeFactory = taskEdgeFactory;
  }

  public Set<TE> getAllTaskEdges(TV sourceTaskVertex, TV targetTaskVertex) {

    return taskGraphSpecifics.getAllTaskEdges(sourceTaskVertex, targetTaskVertex);
  }

  @Override
  public TE getTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {
    return taskGraphSpecifics.getTaskEdge(sourceTaskVertex, targetTaskVertex);
  }

  @Override
  public boolean addTaskEdge(TV taskVertex1, TV taskVertex2, TE taskEdge) {

    boolean success = false;
    if (taskEdge == null) {
      throw new NullPointerException();
    }

    try {
      TE taskdge = taskEdgeFactory.createTaskEdge(taskVertex1, taskVertex2);
      if (containsTaskEdge(taskEdge)) {
        success = false;
      } else {
        IntrusiveTaskEdge intrusiveTaskEdge =
            createIntrusiveTaskEdge(taskEdge, taskVertex1, taskVertex2);
        taskEdgeMap.put(taskEdge, intrusiveTaskEdge);
        success = true;
      }
    } catch (IllegalAccessException iae) {
      iae.printStackTrace();
    } catch (InstantiationException ie) {
      ie.printStackTrace();
    }
    System.out.println("success flag message is:" + success);
    return success;

  }

  public boolean removeTaskEdge(TE taskEdge) {
    /*if (containsTaskEdge (taskEdge)) {
      taskGraphSpecifics.removeTaskEdgeFromTouchingVertices (taskEdge);
      taskEdgeMap.remove (taskEdge);
      return true;
    } else {
      return false;
    }*/
    return true;
  }

  private IntrusiveTaskEdge createIntrusiveTaskEdge(TE taskEdge, TV taskVertex1, TV taskVertex2) {

    IntrusiveTaskEdge intrusiveTaskEdge;

    if (taskEdge instanceof IntrusiveTaskEdge) {
      intrusiveTaskEdge = (IntrusiveTaskEdge) taskEdge;
      System.out.println("I am in intrusive task edge creation if loop:" + intrusiveTaskEdge);
    } else {
      intrusiveTaskEdge = new IntrusiveTaskEdge();
      System.out.println("I am in intrusive task edge creation else loop:" + intrusiveTaskEdge);
    }

    intrusiveTaskEdge.source = taskVertex1;
    intrusiveTaskEdge.target = taskVertex2;

    return intrusiveTaskEdge;
  }

  @Override
  public boolean containsTaskEdge(TV taskVertex1, TV taskVertex2) {
    return false;
  }


  @Override
  public boolean containsTaskEdge(TE taskEdge) {
    return taskEdgeMap.containsKey(taskEdge);
  }


  @Override
  public boolean containsTaskVertex(TV taskVertex) {
    return false;
  }

  @Override
  public TE addTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {

    //check the source and target vertex exists in the dataflow task graph
    TE taskEdge = null;
    try {
      taskEdge = taskEdgeFactory.createTaskEdge(sourceTaskVertex, targetTaskVertex);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    }
    return taskEdge;
  }

  @Override
  public TV getTaskEdgeSource(TE taskEdge) {
      /*return TypeUtil.uncheckedCast(
              getIntrusiveTaskEdge(taskEdge).source,
              vertexTypeDecl);*/

    return vertexTypeDecl.uncheckedCast(
       getIntrusiveTaskEdge(taskEdge).source, vertexTypeDecl);
  }

  @Override
  public TV getTaskEdgeTarget(TE taskEdge) {
    /*return TypeUtil.uncheckedCast(
        getIntrusiveTaskEdge(taskEdge).target,
        vertexTypeDecl);*/

    return vertexTypeDecl.uncheckedCast(
       getIntrusiveTaskEdge(taskEdge).source, vertexTypeDecl);
  }

  public IntrusiveTaskEdge getIntrusiveTaskEdge(TE taskEdge) {
    if (taskEdge instanceof IntrusiveTaskEdge) {
      return (IntrusiveTaskEdge) taskEdge;
    }
    return taskEdgeMap.get(taskEdge);
  }


  public TaskGraphSpecifics createTaskGraphSpecifics() {

    if (this instanceof DataflowTaskGraph<?, ?>) {
      return createDirectedDataflowTaskGraph();
    } else {
      throw new IllegalArgumentException("Dataflow Task Graph must be Directed");
    }
  }

  public DirectedDataflowTaskGraph createDirectedDataflowTaskGraph() {
    return new DirectedDataflowTaskGraph();
  }

  public static class DirectedDataflowTaskEdgeContainer<TV, TE> implements Serializable {

    private static final long serialVersionUID = 2233233333444449278L;

    private Set<TE> incomingTaskEdge;
    private Set<TE> outgoingTaskEdge;
    private transient Set<TE> unmodifiableIncomingTaskEdge = null;
    private transient Set<TE> unmodifiableOutgoingTaskEdge = null;

    DirectedDataflowTaskEdgeContainer(TaskEdgeSetFactory<TV, TE> edgeSetFactory,
                                      TV taskVertex)
        throws InstantiationException, IllegalAccessException {
      try {
        incomingTaskEdge = edgeSetFactory.createTaskEdgeSet(taskVertex);
        outgoingTaskEdge = edgeSetFactory.createTaskEdgeSet(taskVertex);
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      } catch (InstantiationException e) {
        e.printStackTrace();
      }
    }

    public Set<TE> getUnmodifiableIncomingTaskEdges() {
      if (unmodifiableIncomingTaskEdge == null) {
        unmodifiableIncomingTaskEdge = Collections.unmodifiableSet(incomingTaskEdge);
      }
      return unmodifiableIncomingTaskEdge;
    }

    public Set<TE> getUnmodifiableOutgoingTaskEdges() {
      if (unmodifiableOutgoingTaskEdge == null) {
        unmodifiableOutgoingTaskEdge = Collections.unmodifiableSet(outgoingTaskEdge);
      }
      return unmodifiableOutgoingTaskEdge;
    }

    public void addIncomingEdge(TE taskEdge) {
      incomingTaskEdge.add(taskEdge);
    }

    public void addOutgoingEdge(TE taskEdge) {
      outgoingTaskEdge.add(taskEdge);
    }

    public void removeIncomingEdge(TE taskEdge) {
      incomingTaskEdge.remove(taskEdge);
    }

    public void removeOutgoingEdge(TE taskEdge) {
      outgoingTaskEdge.remove(taskEdge);
    }
  }

  public class DirectedDataflowTaskGraph extends TaskGraphSpecifics implements Serializable {

    private static final long serialVersionUID = 56565434344343434L;

    public Map<TV, DirectedDataflowTaskEdgeContainer<TV, TE>> taskVertexMap;

    public DirectedDataflowTaskGraph() {
      this(new LinkedHashMap<TV, DirectedDataflowTaskEdgeContainer<TV, TE>>());
    }

    public DirectedDataflowTaskGraph(Map<TV,
        DirectedDataflowTaskEdgeContainer<TV, TE>> vertexTaskMap) {
      this.taskVertexMap = vertexTaskMap;
    }

    @Override
    public void addTaskVertex(TV taskVertex) {
      taskVertexMap.put(taskVertex, null);
    }

    @Override
    public Set<TV> getTaskVertexSet() {
      return taskVertexMap.keySet();
    }

    @Override
    public Set<TE> getAllTaskEdges(TV sourceTaskVertex, TV targetTaskVertex) {
      Set<TE> taskEdges = null;

      if (containsTaskVertex(sourceTaskVertex)
          && containsTaskVertex(targetTaskVertex)) {

        taskEdges = new ArrayUnenforcedSet<TE>();

        DirectedDataflowTaskEdgeContainer<TV, TE> ec =
            getTaskEdgeContainer(sourceTaskVertex);

        Iterator<TE> iter = ec.outgoingTaskEdge.iterator();

        while (iter.hasNext()) {
          TE e = iter.next();
          if (getTaskEdgeTarget(e).equals(targetTaskVertex)) {
            taskEdges.add(e);
          }
        }
      }
      return taskEdges;
    }

    @Override
    public TE getTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {
      return null;
    }

    @Override
    public void addTaskEdgeToTouchingVertices(TE taskEdge) {

      TV source = getTaskEdgeSource(taskEdge);
      TV target = getTaskEdgeTarget(taskEdge);

      getTaskEdgeContainer(source).addOutgoingEdge(taskEdge);
      getTaskEdgeContainer(target).addIncomingEdge(taskEdge);
    }

    @Override
    public int degreeOf(TV taskVertex) {
      return 0;
    }

    @Override
    public Set<TE> taskEdgesOf(TV taskVertex) {
      ArrayUnenforcedSet<TE> inAndOut =
          new ArrayUnenforcedSet<TE>(getTaskEdgeContainer(taskVertex).incomingTaskEdge);
      inAndOut.addAll(getTaskEdgeContainer(taskVertex).outgoingTaskEdge);

      return Collections.unmodifiableSet(inAndOut);
    }

    @Override
    public int inDegreeOf(TV taskVertex) {
      return getTaskEdgeContainer(taskVertex).incomingTaskEdge.size();
    }

    @Override
    public Set<TE> incomingTaskEdgesOf(TV taskVertex) {
      return getTaskEdgeContainer(taskVertex).getUnmodifiableIncomingTaskEdges();
    }

    @Override
    public int outDegreeOf(TV taskVertex) {
      return getTaskEdgeContainer(taskVertex).outgoingTaskEdge.size();
    }

    @Override
    public Set<TE> outgoingTaskEdgesOf(TV taskVertex) {
      return getTaskEdgeContainer(taskVertex).getUnmodifiableOutgoingTaskEdges();
    }

    @Override
    public void removeTaskEdgeFromTouchingVertices(TE taskEdge) {

      TV source = getTaskEdgeSource(taskEdge);
      TV target = getTaskEdgeSource(taskEdge);

      getTaskEdgeContainer(source).removeOutgoingEdge(taskEdge);
      getTaskEdgeContainer(target).removeIncomingEdge(taskEdge);
    }

    private DirectedDataflowTaskEdgeContainer<TV, TE> getTaskEdgeContainer(TV taskVertex) {

      assertTaskVertexExist(taskVertex);
      DirectedDataflowTaskEdgeContainer<TV, TE> ec = taskVertexMap.get(taskVertex);
      if (ec == null) {
        try {
          ec = new DirectedDataflowTaskEdgeContainer<TV, TE>(taskEdgeSetFactory, taskVertex);
        } catch (InstantiationException e) {
          e.printStackTrace();
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
        taskVertexMap.put(taskVertex, ec);
      }
      return ec;
    }
  }

  private abstract class TaskGraphSpecifics implements Serializable {

    private static final long serialVersionUID = 33323434344343434L;

    public abstract void addTaskVertex(TV taskVertex);

    public abstract Set<TV> getTaskVertexSet();

    public abstract Set<TE> getAllTaskEdges(TV sourceTaskVertex,
                                            TV targetTaskVertex);

    public abstract TE getTaskEdge(TV sourceTaskVertex, TV targetTaskVertex);

    public abstract void addTaskEdgeToTouchingVertices(TE taskEdge);

    public abstract int degreeOf(TV taskVertex);

    public abstract Set<TE> taskEdgesOf(TV taskVertex);

    public abstract int inDegreeOf(TV taskVertex);

    public abstract Set<TE> incomingTaskEdgesOf(TV taskVertex);

    public abstract int outDegreeOf(TV taskVertex);

    public abstract Set<TE> outgoingTaskEdgesOf(TV taskVertex);

    public abstract void removeTaskEdgeFromTouchingVertices(TE taskEdge);
  }

  private class ArrayListFactory<TV, TE> implements TaskEdgeSetFactory<TV, TE> {

    public Set<TE> createTaskEdgeSet(TV taskVertex) {
      return new ArrayUnenforcedSet<TE>(1);
    }
  }
}
