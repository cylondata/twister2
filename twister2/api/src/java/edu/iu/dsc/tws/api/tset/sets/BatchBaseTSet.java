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

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.PartitionFunction;
import edu.iu.dsc.tws.api.tset.ReduceFunction;
import edu.iu.dsc.tws.api.tset.Selector;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.link.AllGatherTLink;
import edu.iu.dsc.tws.api.tset.link.AllReduceTLink;
import edu.iu.dsc.tws.api.tset.link.DirectTLink;
import edu.iu.dsc.tws.api.tset.link.GatherTLink;
import edu.iu.dsc.tws.api.tset.link.PartitionTLink;
import edu.iu.dsc.tws.api.tset.link.ReduceTLink;
import edu.iu.dsc.tws.api.tset.link.ReplicateTLink;
import edu.iu.dsc.tws.common.config.Config;

public abstract class BatchBaseTSet<T> extends BaseTSet<T> {
  private static final Logger LOG = Logger.getLogger(BatchBaseTSet.class.getName());

  public BatchBaseTSet(Config cfg, TSetEnv tSetEnv) {
    super(cfg, tSetEnv);
  }

  @Override
  public DirectTLink<T> direct() {
    DirectTLink<T> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    return direct;
  }

  @Override
  public ReduceTLink<T> reduce(ReduceFunction<T> reduceFn) {
    ReduceTLink<T> reduce = new ReduceTLink<T>(config, tSetEnv, this, reduceFn);
    children.add(reduce);
    return reduce;
  }

  public PartitionTLink<T> partition(PartitionFunction<T> partitionFn) {
    PartitionTLink<T> partition = new PartitionTLink<>(config, tSetEnv, this, partitionFn);
    children.add(partition);
    return partition;
  }

  @Override
  public GatherTLink<T> gather() {
    GatherTLink<T> gather = new GatherTLink<>(config, tSetEnv, this);
    children.add(gather);
    return gather;
  }

  @Override
  public AllReduceTLink<T> allReduce(ReduceFunction<T> reduceFn) {
    AllReduceTLink<T> reduce = new AllReduceTLink<>(config, tSetEnv, this, reduceFn);
    children.add(reduce);
    return reduce;
  }

  @Override
  public AllGatherTLink<T> allGather() {
    AllGatherTLink<T> gather = new AllGatherTLink<>(config, tSetEnv, this);
    children.add(gather);
    return gather;
  }

  @Override
  public <K> GroupedTSet<T, K> groupBy(PartitionFunction<K> partitionFunction,
                                       Selector<T, K> selector) {
    GroupedTSet<T, K> groupedTSet = new GroupedTSet<>(config, tSetEnv, this,
        partitionFunction, selector);
    children.add(groupedTSet);
    return groupedTSet;
  }

  @Override
  public ReplicateTLink<T> replicate(int replications) {
    if (parallel != 1) {
      String msg = "TSets with parallelism 1 can be replicated: " + parallel;
      LOG.log(Level.SEVERE, msg);
      throw new RuntimeException(msg);
    }

    ReplicateTLink<T> cloneTSet = new ReplicateTLink<>(config, tSetEnv, this, replications);
    children.add(cloneTSet);
    return cloneTSet;
  }

  @Override
  public CachedTSet<T> cache() {
    // todo: why cant we add a single cache tset here?
    DirectTLink<T> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    CachedTSet<T> cacheTSet = new CachedTSet<>(config, tSetEnv, direct, parallel);
    direct.getChildren().add(cacheTSet);
    cacheTSet.setData(tSetEnv.runAndGet(cacheTSet.getName()));

    tSetEnv.reset();
    return cacheTSet;
  }


}
