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
    boolean success = false;
    if (!(getTaskEdge(sourceTaskVertex, targetTaskVertex) == null)) {
      success = true;
    }
    return success;
  }

  @Override
  public boolean removeAllTaskEdges(Collection<? extends TE> taskEdges) {
    boolean success = false;
    for (TE taskEdge : taskEdges) {
      // success |= removeTaskEdge(taskEdge);
    }
    return success;
  }

  @Override
  public Set<TE> removeAllTaskEdges(TV sourceTaskVertex,
                                    TV targetTaskVertex) {
    return null;
  }

  protected boolean removeAllTaskEdges(TE[] taskEdges) {
    boolean success = false;
    //do the computation to remove all the task edges....!
    return success;
  }

  @Override
  public boolean removeAllTaskVertices(Collection<? extends TV>
                                           taskVertices) {
    boolean success = false;
    //do the computation to remove the vertex....
    return success;
  }


}
