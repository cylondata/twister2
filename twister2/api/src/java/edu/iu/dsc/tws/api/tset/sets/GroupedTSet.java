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

package edu.iu.dsc.tws.api.tset.sets;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.Selector;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunction;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunction;
import edu.iu.dsc.tws.api.tset.link.KeyedGatherTLink;
import edu.iu.dsc.tws.api.tset.link.KeyedPartitionTLink;
import edu.iu.dsc.tws.api.tset.link.KeyedReduceTLink;
import edu.iu.dsc.tws.common.config.Config;

public class GroupedTSet<K, V> extends BatchBaseTSet<V> {
  private PartitionFunction<K> partitioner;

  private Selector<K, V> selector;

  private BaseTSet<V> parent;

  public GroupedTSet(Config cfg, TSetEnv tSetEnv, BaseTSet<V> prnt, PartitionFunction<K> partFn,
                     Selector<K, V> selc) {
    super(cfg, tSetEnv);
    this.partitioner = partFn;
    this.selector = selc;
    this.parent = prnt;
    this.name = "grouped-" + parent.getName();
    this.parallel = 1;
  }

  public GroupedTSet(Config cfg, TSetEnv tSetEnv, BaseTSet<V> prnt, PartitionFunction<K> partFn,
                     Selector<K, V> selc, int parallelism) {
    super(cfg, tSetEnv);
    this.partitioner = partFn;
    this.selector = selc;
    this.parent = prnt;
    this.name = "grouped-" + parent.getName();
    this.parallel = parallelism;
  }

  public KeyedReduceTLink<K, V> keyedReduce(ReduceFunction<V> reduceFn) {
    KeyedReduceTLink<K, V> reduce = new KeyedReduceTLink<>(config, tSetEnv, parent,
        reduceFn, partitioner, selector);
    children.add(reduce);
    return reduce;
  }

  public KeyedPartitionTLink<K, V> keyedPartition() {
    KeyedPartitionTLink<K, V> partition = new KeyedPartitionTLink<>(config, tSetEnv,
        parent, partitioner, selector);
    children.add(partition);
    return partition;
  }

  public KeyedGatherTLink<K, V> keyedGather() {
    KeyedGatherTLink<K, V> gather = new KeyedGatherTLink<>(config, tSetEnv, parent,
        partitioner, selector);
    children.add(gather);
    return gather;
  }

  public BaseTSet<V> getParent() {
    return parent;
  }

  @Override
  public boolean baseBuild() {
    return true;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
    throw new IllegalStateException("Build connections should not be called on a TSet");
  }

  @Override
  public GroupedTSet<K, V> setName(String n) {
    this.name = n;
    return this;
  }
}
