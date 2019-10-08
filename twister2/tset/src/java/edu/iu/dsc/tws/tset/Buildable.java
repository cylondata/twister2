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
package edu.iu.dsc.tws.tset;

import java.util.Collection;

import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;

/**
 * Interface that provides buildable functionality for sets and links
 */
public interface Buildable {

  /**
   * Builds the underlying node or edge
   *
   * @param buildSequence partial build sequence. i.e. the set of nodes and edges that have been
   *                        build so far, by traversing the tset subgraph
   */
  void build(Collection<? extends TBase> buildSequence);

  /**
   * tset env
   *
   * @return tset env
   */
  TSetEnvironment getTSetEnv();

  /**
   * tbase graph
   *
   * @return tbase graph
   */
  default TBaseGraph getTBaseGraph() {
    return getTSetEnv().getGraph();
  }

  /**
   * generates a unique ID for tsets or tlinks.
   * <p>
   * todo: check how this behaves with fault tolerance
   *
   * @param prefix prefix
   * @return ID
   */
  default String generateID(String prefix) {
    return "__" + prefix + getTBaseGraph().getNodes().size();
  }
}
