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

package edu.iu.dsc.tws.tset.links.batch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.JoinedTuple;
import edu.iu.dsc.tws.api.compute.OperationNames;
import edu.iu.dsc.tws.api.compute.TaskPartitioner;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.sets.TupleTSet;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.fn.HashingPartitioner;
import edu.iu.dsc.tws.tset.sets.BuildableTSet;

public class JoinTLink<K, VL, VR> extends BatchIteratorLinkWrapper<JoinedTuple<K, VL, VR>> {

  private CommunicationContext.JoinType joinType;
  private TaskPartitioner<K> partitioner;
  private Comparator<K> keyComparator;

  private CommunicationContext.JoinAlgorithm algorithm = CommunicationContext.JoinAlgorithm.SORT;
  private MessageType keyType = MessageTypes.OBJECT;

  private TupleTSet leftTSet;
  private TupleTSet rightTSet;
  private boolean useDisk = false;

  // guava graph does not guarantee the insertion order for predecessors and successors. hence
  // the left and right tsets needs to be taken in explicitly
  public JoinTLink(BatchTSetEnvironment env, CommunicationContext.JoinType type,
                   Comparator<K> kComparator, TupleTSet leftT, TupleTSet rightT) {
    this(env, type, kComparator, new HashingPartitioner<>(), leftT, rightT);
  }

  public JoinTLink(BatchTSetEnvironment env, CommunicationContext.JoinType type,
                   Comparator<K> kComparator, TaskPartitioner<K> partitioner, TupleTSet leftT,
                   TupleTSet rightT) {
    super(env, "join", ((BuildableTSet) leftT).getParallelism());
    this.joinType = type;
    this.leftTSet = leftT;
    this.rightTSet = rightT;
    this.keyComparator = kComparator;
    this.partitioner = partitioner;
  }

  @Override
  public JoinTLink<K, VL, VR> setName(String name) {
    rename(name);
    return this;
  }

  @Override
  public JoinTLink<K, VL, VR> withDataType(MessageType dataType) {
    return (JoinTLink<K, VL, VR>) super.withDataType(dataType);
  }

  @Override
  public Edge getEdge() {
    return new Edge(getId(), OperationNames.JOIN, getDataType());
  }

  @Override
  public void build(GraphBuilder graphBuilder, Collection<? extends TBase> buildSequence) {

    // filter out the relevant sources out of the predecessors
    ArrayList<TBase> sources = new ArrayList<>(getTBaseGraph().getPredecessors(this));
    sources.retainAll(buildSequence);

    if (sources.size() != 2) {
      throw new RuntimeException("Join TLink predecessor count should be 2: Received "
          + sources.size());
    }

    // filter out the relevant sources out of the successors
    HashSet<TBase> targets = new HashSet<>(getTBaseGraph().getSuccessors(this));
    targets.retainAll(buildSequence);

    for (TBase target : targets) {
      // group name = left_right_join_target
      String groupName = leftTSet.getId() + "_" + rightTSet.getId() + "_" + getId() + "_"
          + target.getId();

      // build left
      buildJoin(graphBuilder, leftTSet, target, 0, groupName);

      // build right
      buildJoin(graphBuilder, rightTSet, target, 1, groupName);
    }
  }

  private void buildJoin(GraphBuilder graphBuilder, TBase s, TBase t, int idx, String groupName) {
    Edge e = getEdge();
    // override edge name with join_source_target
    e.setName(e.getName() + "_" + s.getId() + "_" + t.getId());
    e.setKeyed(true);
    e.setPartitioner(partitioner);

    e.setEdgeIndex(idx);
    e.setNumberOfEdges(2);
    e.setTargetEdge(groupName);
    e.addProperty(CommunicationContext.JOIN_TYPE, joinType);
    e.addProperty(CommunicationContext.JOIN_ALGORITHM, algorithm);
    e.addProperty(CommunicationContext.KEY_COMPARATOR, keyComparator);
    e.addProperty(CommunicationContext.USE_DISK, useDisk);
    e.setKeyType(keyType);

    graphBuilder.connect(s.getId(), t.getId(), e);
  }

  public JoinTLink<K, VL, VR> useHashAlgorithm(MessageType kType) {
    this.algorithm = CommunicationContext.JoinAlgorithm.HASH;
    this.keyType = kType;
    return this;
  }

  public JoinTLink<K, VL, VR> useDisk() {
    this.useDisk = true;
    return this;
  }
}
