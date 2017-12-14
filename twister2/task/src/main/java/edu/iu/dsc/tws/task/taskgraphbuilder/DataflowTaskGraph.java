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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class DataflowTaskGraph<TV, TE> extends AbstractDataflowTaskGraphImpl<TV, TE>
    implements IDataflowTaskGraph<TV, TE> {

  private static final long serialVersionUID = 2233233333444449278L;

  public Map<TV, DataflowEdge<TV, TE>> vertexMapDataflow = new LinkedHashMap<>();

  public DataflowTaskGraph(Class<? extends TE> taskEdgeClass) {
    this(new DataflowTaskEdgeFactory<TV, TE>(taskEdgeClass));
  }

  public DataflowTaskGraph(IDataflowTaskEdgeFactory<TV, TE> taskEdgeFactory) {
    super(taskEdgeFactory);
  }


  public boolean addTaskVertex(TV taskVertex) {
    vertexMapDataflow.put(taskVertex, null);
    System.out.println("TaskVertex Map Details are:" + vertexMapDataflow);
    return true;
  }

  @Override
  public Set<TV> getTaskVertexSet() {
    return vertexMapDataflow.keySet();
  }

  @Override
  public Set<TE> taskEdgeSet() {
    return null;
  }

  @Override
  public TE removeTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {
    return null;
  }

  @Override
  public boolean removeTaskVertex(TV taskVertex) {
    return false;
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

  /*@Override
  public DataflowTaskGraph createDirectedDataflowTaskGraph() {
    return null;
  }*/

  private class DataflowEdge<TV, TE> {

  }
}

