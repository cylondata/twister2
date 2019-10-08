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
package edu.iu.dsc.tws.tset.env;

import java.util.Set;

import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.tset.sets.BuildableTSet;

/**
 * TSet build context holds references for executed tset, build sequence of that tset, compute
 * graph, inputs(?), output data obj (?)
 */
public class TBaseBuildContext {
//  private static final long serialVersionUID = -3016894342731475383L;

  private String buildId;
  private Set<BuildableTSet> rootTBases;
  private Set<TBase> buildSequence;
  private ComputeGraph computeGraph;
//  private List<DataObject> inputs, outputs;

  public TBaseBuildContext(String bId, Set<BuildableTSet> roots, Set<TBase> buildSeq,
                           ComputeGraph compGraph) {
    this.buildId = bId;
    this.rootTBases = roots;
    this.buildSequence = buildSeq;
    this.computeGraph = compGraph;
  }

  public ComputeGraph getComputeGraph() {
    return computeGraph;
  }

  public String getBuildId() {
    return buildId;
  }

  public Set<BuildableTSet> getRoots() {
    return rootTBases;
  }

  public Set<TBase> getBuildSequence() {
    return buildSequence;
  }
}
