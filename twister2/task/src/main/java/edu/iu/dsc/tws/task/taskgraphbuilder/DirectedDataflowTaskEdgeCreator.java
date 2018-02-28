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

import java.util.Collections;
import java.util.Set;

public class DirectedDataflowTaskEdgeCreator<TV, TE> {

  protected Set<TE> incomingTaskEdge;
  protected Set<TE> outgoingTaskEdge;

  private Set<TE> iIncomingTaskEdge = null;
  private Set<TE> iOutgoingTaskEdge = null;

  DirectedDataflowTaskEdgeCreator(IDataflowTaskEdgeSetFactory<TV, TE> edgeSetFactory,
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

  public Set<TE> getImmutableIncomingTaskEdges() {
    if (iIncomingTaskEdge == null) {
      iIncomingTaskEdge = Collections.unmodifiableSet(incomingTaskEdge);
    }
    return iIncomingTaskEdge;
  }

  public Set<TE> getImmutableOutgoingTaskEdges() {
    if (iOutgoingTaskEdge == null) {
      iOutgoingTaskEdge = Collections.unmodifiableSet(outgoingTaskEdge);
    }
    return iOutgoingTaskEdge;
  }

  public void addIncomingTaskEdge(TE taskEdge) {
    incomingTaskEdge.add(taskEdge);
  }

  public void addOutgoingTaskEdge(TE taskEdge) {
    outgoingTaskEdge.add(taskEdge);
  }

  public void removeIncomingTaskEdge(TE taskEdge) {
    incomingTaskEdge.remove(taskEdge);
  }

  public void removeOutgoingTaskEdge(TE taskEdge) {
    outgoingTaskEdge.remove(taskEdge);
  }
}
