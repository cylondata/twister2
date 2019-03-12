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
import edu.iu.dsc.tws.api.tset.PartitionFunction;
import edu.iu.dsc.tws.api.tset.ReduceFunction;
import edu.iu.dsc.tws.api.tset.Selector;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.link.KeyedGatherTLink;
import edu.iu.dsc.tws.api.tset.link.KeyedPartitionTLink;
import edu.iu.dsc.tws.api.tset.link.KeyedReduceTLink;
import edu.iu.dsc.tws.common.config.Config;

public class GroupedTSet<T, K> extends BaseTSet<T> {
  private PartitionFunction<K> partitioner;

  private Selector<T, K> selector;

  private BaseTSet<T> parent;

  public GroupedTSet(Config cfg, TSetEnv tSetEnv, BaseTSet<T> prnt, PartitionFunction<K> partFn,
                     Selector<T, K> selc) {
    super(cfg, tSetEnv);
    this.partitioner = partFn;
    this.selector = selc;
    this.parent = prnt;
    this.name = "grouped-" + parent.getName();
  }

  public KeyedReduceTLink<T, K> keyedReduce(ReduceFunction<T> reduceFn) {
    KeyedReduceTLink<T, K> reduce = new KeyedReduceTLink<>(config, tSetEnv, parent,
        reduceFn, partitioner, selector);
    children.add(reduce);
    return reduce;
  }

  public KeyedPartitionTLink<T, K> keyedPartition() {
    KeyedPartitionTLink<T, K> partition = new KeyedPartitionTLink<>(config, tSetEnv,
        parent, partitioner, selector);
    children.add(partition);
    return partition;
  }

  public KeyedGatherTLink<T, K> keyedGather() {
    KeyedGatherTLink<T, K> gather = new KeyedGatherTLink<>(config, tSetEnv, parent,
        partitioner, selector);
    children.add(gather);
    return gather;
  }

  public BaseTSet<T> getParent() {
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
  public GroupedTSet<T, K> setParallelism(int parallelism) {
    this.parallel = parallelism;
    return this;
  }

  @Override
  public GroupedTSet<T, K> setName(String n) {
    this.name = n;
    return this;
  }
}
