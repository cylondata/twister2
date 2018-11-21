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
package edu.iu.dsc.tws.task.graph.htgraph;

import java.util.Comparator;

public class HTGDirectedEdge<TG, TE> {

  protected TG sourceTaskGraph;
  protected TG targetTaskGraph;
  protected TE taskGraphEdge;

  protected Comparator<TG> taskGraphComparator;

  public HTGDirectedEdge() {
  }

  public HTGDirectedEdge(Comparator<TG> taskgraphComparator) {
    this.taskGraphComparator = taskgraphComparator;
  }

  public HTGDirectedEdge(TG sourceTaskgraph, TG targetTaskgraph,
                         TE taskgraphEdge, Comparator<TG> taskgraphComparator) {

    this.sourceTaskGraph = sourceTaskgraph;
    this.targetTaskGraph = targetTaskgraph;
    this.taskGraphEdge = taskgraphEdge;
    this.taskGraphComparator = taskgraphComparator;
  }

}
