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
package edu.iu.dsc.tws.api.htgjob;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class Twister2MetagraphConnection {

  private static final Logger LOG = Logger.getLogger(Twister2MetagraphConnection.class.getName());

  private String nodeName;

  private Map<String, Twister2Metagraph.Relation> inputs = new HashMap<>();

  public Twister2MetagraphConnection(String nodeName) {
    this.nodeName = nodeName;
  }

  /**
   * Create a broadcast connection
   */
  public Twister2MetagraphConnection broadcast(String parent, String name) {
    Twister2Metagraph.Relation relation = new Twister2Metagraph.Relation(parent, name);
    inputs.put(parent, relation);
    return this;
  }

  void buildCompute(Twister2Metagraph graph) {
    for (Map.Entry<String, Twister2Metagraph.Relation> e : inputs.entrySet()) {
      Twister2Metagraph.SubGraph graph1 = graph.getMetaGraphMap(nodeName);
      if (graph1 == null) {
        throw new RuntimeException("Failed to connect non-existing subgraph: " + nodeName);
      }

      Twister2Metagraph.SubGraph graph2 = graph.getMetaGraphMap(e.getKey());
      if (graph2 == null) {
        throw new RuntimeException("Failed to connect non-existing subgraph:" + e.getKey());
      }

      LOG.fine("Graph name:" + graph2.getName() + "\t" + graph1.getName()
          + "\tRelation Name:" + e.getValue().getOperation());
      graph.addRelation(graph2.getName(), graph1.getName(), e.getValue());
    }
  }

  void buildSource(Twister2Metagraph metaGraph) {
  }

}
