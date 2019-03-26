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


import edu.iu.dsc.tws.api.tset.PartitionFunction;
import edu.iu.dsc.tws.api.tset.ReduceFunction;
import edu.iu.dsc.tws.api.tset.Selector;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.link.AllGatherTLink;
import edu.iu.dsc.tws.api.tset.link.AllReduceTLink;
import edu.iu.dsc.tws.api.tset.link.GatherTLink;
import edu.iu.dsc.tws.api.tset.link.ReduceTLink;
import edu.iu.dsc.tws.api.tset.link.ReplicateTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingDirectTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingPartitionTLink;
import edu.iu.dsc.tws.common.config.Config;

public abstract class StreamingBaseTSet<T> extends BaseTSet<T> {
  public StreamingBaseTSet(Config cfg, TSetEnv tSetEnv) {
    super(cfg, tSetEnv);
  }

  @Override
  public StreamingDirectTLink<T> direct() {
    StreamingDirectTLink<T> direct = new StreamingDirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    return direct;
  }

  @Override
  public ReduceTLink<T> reduce(ReduceFunction<T> reduceFn) {
    return null;
  }

  public StreamingPartitionTLink<T> partition(PartitionFunction<T> partitionFn) {
    StreamingPartitionTLink<T> partition = new StreamingPartitionTLink<>(config,
        tSetEnv, this, partitionFn);
    children.add(partition);
    return partition;
  }

  @Override
  public GatherTLink<T> gather() {
    return null;

  }

  @Override
  public AllReduceTLink<T> allReduce(ReduceFunction<T> reduceFn) {
    return null;

  }

  @Override
  public AllGatherTLink<T> allGather() {
    return null;

  }

  @Override
  public <K> GroupedTSet<T, K> groupBy(PartitionFunction<K> partitionFunction,
                                       Selector<T, K> selector) {
    return null;

  }

  @Override
  public ReplicateTLink<T> replicate(int replications) {
    return null;
  }

  @Override
  public CachedTSet<T> cache() {
    throw new UnsupportedOperationException("Cache is not avilable in streaming operations");
  }
}
