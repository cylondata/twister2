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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public abstract class AbstractTaskGraphImpl<TV, TE> extends AbstractTaskGraph<TV, TE>
    implements TaskGraph<TV, TE>, Cloneable, Serializable {

  private static final long serialVersionUID = 223333333344444666L;

  private TaskEdgeFactory<TV, TE> taskEdgeFactory;
  private TaskGraphSpecifics taskGraphSpecifics;
  private Map<TE, IntrusiveTaskEdge> taskEdgeMap;
  private TaskEdgeSetFactory<TV, TE> taskEdgeSetFactory;


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
      TE taskedge = taskEdgeFactory.createTaskEdge(taskVertex1, taskVertex2);
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

  private IntrusiveTaskEdge createIntrusiveTaskEdge(TE taskEdge, TV taskVertex1, TV taskVertex2) {

    IntrusiveTaskEdge intrusiveTaskEdge;

    if (taskEdge instanceof IntrusiveTaskEdge) {
      intrusiveTaskEdge = (IntrusiveTaskEdge) taskEdge;
      System.out.println("I am in intrusive task edge creation if loop:" + intrusiveTaskEdge);
    } else {
      intrusiveTaskEdge = new IntrusiveTaskEdge();
      System.out.println("I am in intrusive task edge creation else loop:" + intrusiveTaskEdge);
    }

    //intrusiveTaskEdge.source = taskVertex1;
    //intrusiveTaskEdge.target = taskVertex2;

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


  public class DirectedDataflowTaskGraph extends TaskGraphSpecifics implements Serializable {

    private static final long serialVersionUID = 2233233333444449278L;

    public Map<TV, DirectedDataflowTaskEdgeContainer<TV, TE>> vertexTaskMapDirected;

    public DirectedDataflowTaskGraph() {
      this(new LinkedHashMap<TV, DirectedDataflowTaskEdgeContainer<TV, TE>>());
    }

    public DirectedDataflowTaskGraph(Map<TV,
        DirectedDataflowTaskEdgeContainer<TV, TE>> vertexTaskMap) {
      this.vertexTaskMapDirected = vertexTaskMap;
    }

    @Override
    public void addTaskVertex(TV taskVertex) {
      vertexTaskMapDirected.put(taskVertex, null);
    }

    @Override
    public Set<TV> getTaskVertexSet() {
      return vertexTaskMapDirected.keySet();
    }

    @Override
    public Set<TE> getAllTaskEdges(TV sourceTaskVertex, TV targetTaskVertex) {
      return null;
    }

    @Override
    public TE getTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {
      return null;
    }

    @Override
    public void addTaskEdgeToTouchingVertices(TE taskEdge) {
    }

    @Override
    public int degreeOf(TV taskVertex) {
      return 0;
    }

    @Override
    public Set<TE> taskEdgesOf(TV taskVertex) {
      return null;
    }

    @Override
    public int inDegreeOf(TV taskVertex) {
      return 0;
    }

    @Override
    public Set<TE> incomingTaskEdgesOf(TV taskVertex) {
      return null;
    }

    @Override
    public int outDegreeOf(TV taskVertex) {
      return 0;
    }

    @Override
    public Set<TE> outgoingTaskEdgesOf(TV taskVertex) {
      return null;
    }

    @Override
    public void removeTaskEdgeFromTouchingVertices(TE taskEdge) {
    }
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

    public Set<TE> getUnmodifiableOutgoingEdges() {
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

  private abstract class TaskGraphSpecifics implements Serializable {

    private static final long serialVersionUID = 2233233333444449278L;

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
      return new TreeSet<>();
    }
  }

    /*public class DataflowTaskGraphContainer<TV, TE> implements Serializable {

        Set<TE> incomingTaskEdges;
        Set<TE> outgoingTaskEdges;

        DataflowTaskGraphContainer(TaskEdgeSetFactory<TV, TE> taskEdgeSetFactory, TV taskVertex) {
            try {
                incomingTaskEdges = taskEdgeSetFactory.createTaskEdgeSet (taskVertex);
                outgoingTaskEdges = taskEdgeSetFactory.createTaskEdgeSet (taskVertex);
            } catch (IllegalAccessException e) {
                e.printStackTrace ();
            } catch (InstantiationException e) {
                e.printStackTrace ();
            }
        }

        public void setIncomingTaskEdges(TE taskEdge) {
            incomingTaskEdges.add (taskEdge);
        }

        public void setOutgoingTaskEdges(TE taskEdge) {
            outgoingTaskEdges.add (taskEdge);
        }


        public void removeIncomingTaskEdges(TE taskEdge) {
            incomingTaskEdges.remove (taskEdge);
        }


        public void removeOutgoingTaskEdges(TE taskEdge) {
            outgoingTaskEdges.remove (taskEdge);
        }
    }*/
}
