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
package edu.iu.dsc.tws.api.tset.link.batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.structs.JoinedTuple;
import edu.iu.dsc.tws.api.task.OperationNames;
import edu.iu.dsc.tws.api.task.TaskPartitioner;
import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.TSetGraph;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.LoadBalancePartitioner;

public class JoinTLink<K, VL, VR> extends BIteratorLink<JoinedTuple<K, VL, VR>> {
  private CommunicationContext.JoinType joinType;

  private TaskPartitioner partitioner = new LoadBalancePartitioner();

  public JoinTLink(BatchTSetEnvironment env, CommunicationContext.JoinType type, int sourceP) {
    super(env, TSetUtils.generateName("join"), sourceP);
    this.joinType = type;
  }

  @Override
  public JoinTLink<K, VL, VR> setName(String name) {
    rename(name);
    return this;
  }

  @Override
  public Edge getEdge() {
    return new Edge(getName(), OperationNames.JOIN, getMessageType());
  }

  @Override
  public void build(TSetGraph tSetGraph, Collection<? extends TBase> tSets) {

    // filter out the relevant sources out of the predecessors
    ArrayList<TBase> sources = new ArrayList<>(tSetGraph.getPredecessors(this));
    sources.retainAll(tSets);

    // filter out the relevant sources out of the successors
    HashSet<TBase> targets = new HashSet<>(tSetGraph.getSuccessors(this));
    targets.retainAll(tSets);

    if (sources.size() != 2) {
      throw new RuntimeException("Join TLink predecessor count should be 2: Received "
          + sources.size());
    }

    TBase left = sources.get(0);
    TBase right = sources.get(1);

    for (TBase target : targets) {
      // group name = left_right_join_target
      String groupName = left.getName() + "_" + right.getName() + "_" + getName() + "_"
          + target.getName();

      // build left
      buildJoin(tSetGraph, left, target, 0, groupName);

      // build right
      buildJoin(tSetGraph, right, target, 1, groupName);
    }
  }

  private void buildJoin(TSetGraph tSetGraph, TBase s, TBase t, int idx, String groupName) {
    Edge e = getEdge();
    // override edge name with join_source_target
    e.setName(e.getName() + "_" + s.getName() + "_" + t.getName());
    e.setKeyed(true);
    e.setPartitioner(partitioner);

    e.setEdgeIndex(idx);
    e.setNumberOfEdges(2);
    e.setTargetEdge(groupName);

    tSetGraph.getDfwGraphBuilder().connect(s.getName(), t.getName(), e);
  }
}
