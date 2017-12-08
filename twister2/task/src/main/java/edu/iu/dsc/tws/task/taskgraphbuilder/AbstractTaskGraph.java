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

import java.util.Collection;
import java.util.Set;

public abstract class AbstractTaskGraph<TV, TE> implements TaskGraph<TV, TE> {
  public AbstractTaskGraph() {
  }

  @Override
  public boolean containsTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {
    boolean flag = false;
    if (!(getTaskEdge(sourceTaskVertex, targetTaskVertex) == null)) {
      flag = true;
    }
    return flag;
  }

  @Override
  public boolean removeAllTaskEdges(Collection<? extends TE> taskEdges) {
    boolean success = false;
    for (TE taskEdge : taskEdges) {
      //success |= removeTaskEdge (taskEdge);
    }
    return success;
  }

  protected boolean removeAllTaskEdges(TE[] taskEdges) {
    boolean success = false;
    for (int i = 0; i < taskEdges.length; i++) {
      //success |= removeTaskEdge(taskEdges[i]);
    }
    return success;
  }

  @Override
  public Set<TE> removeAllTaskEdges(TV sourceTaskVertex,
                                    TV targetTaskVertex) {
    Set<TE> removedTaskEdge = getAllTaskEdges(sourceTaskVertex, targetTaskVertex);
    if (removedTaskEdge == null) {
      return null;
    }
    removeAllTaskEdges(removedTaskEdge);
    return removedTaskEdge;
  }

  @Override
  public boolean removeAllTaskVertices(Collection<? extends TV>
                                           taskVertices) {
    boolean flag = false;
    for (TV taskVertex : taskVertices) {
      flag |= removeTaskVertex(taskVertex);
    }
    return flag;
  }

  protected boolean assertTaskVertexExist(TV taskVertex) {
    if (containsTaskVertex(taskVertex)) {
      return true;
    } else if (taskVertex == null) {
      throw new NullPointerException();
    } else {
      throw new IllegalArgumentException(
          "no such vertex in graph: " + taskVertex.toString());
    }
  }
}



