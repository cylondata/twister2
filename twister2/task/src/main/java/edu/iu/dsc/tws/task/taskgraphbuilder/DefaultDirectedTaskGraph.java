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

public class DefaultDirectedTaskGraph<TV, TE> extends AbstractTaskGraph<TV, TE>
                                  implements TaskGraph<TV, TE>, Cloneable, Serializable {

  private static final long serialVersionUID = 3618135658586388792L;

  public DefaultDirectedTaskGraph(TaskEdgeFactory<TV, TE> taskEdgeFactory) {
    super(taskEdgeFactory);
  }

  public DefaultDirectedTaskGraph(Class<? extends TE> taskEdgeClass) {
    this(new TaskEdgeFactory<TV, TE>() {
      @Override
      public TE createEdge(TV sourceTaskVertex, TV targetTaskVertex) {
        return null;
      }
    });
  }

}



