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
package edu.iu.dsc.tws.api.tset.sets.streaming;


import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.Selector;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunction;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunction;
import edu.iu.dsc.tws.api.tset.link.ReduceTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingAllGatherTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingAllReduceTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingDirectTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingGatherTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingPartitionTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingReplicateTLink;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.CachedTSet;
import edu.iu.dsc.tws.api.tset.sets.GroupedTSet;
import edu.iu.dsc.tws.common.config.Config;

public abstract class StreamingBaseTSet<T> extends BaseTSet<T> {
  private static final Logger LOG = Logger.getLogger(StreamingBaseTSet.class.getName());

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
  public StreamingGatherTLink<T> gather() {
    StreamingGatherTLink<T> gather = new StreamingGatherTLink<>(config,
        tSetEnv, this);
    children.add(gather);
    return gather;
  }

  @Override
  public StreamingAllReduceTLink<T> allReduce(ReduceFunction<T> reduceFn) {
    StreamingAllReduceTLink<T> allreduce = new StreamingAllReduceTLink<>(config, tSetEnv,
        this, reduceFn);
    children.add(allreduce);
    return allreduce;
  }

  @Override
  public StreamingAllGatherTLink<T> allGather() {
    StreamingAllGatherTLink<T> allgather = new StreamingAllGatherTLink<>(config,
        tSetEnv, this);
    children.add(allgather);
    return allgather;
  }

  @Override
  public <K> GroupedTSet<K, T> groupBy(PartitionFunction<K> partitionFunction,
                                       Selector<K, T> selector) {
    throw new UnsupportedOperationException("Cache is not avilable in streaming operations");
  }

  @Override
  public StreamingReplicateTLink<T> replicate(int replications) {

    if (parallel != 1) {
      String msg = "TSets with parallelism 1 can be replicated: " + parallel;
      LOG.log(Level.SEVERE, msg);
      throw new RuntimeException(msg);
    }
    StreamingReplicateTLink<T> cloneTSet = new StreamingReplicateTLink<>(config, tSetEnv,
        this, replications);
    children.add(cloneTSet);
    return cloneTSet;
  }

  @Override
  public CachedTSet<T> cache() {
    throw new UnsupportedOperationException("Cache is not avilable in streaming operations");
  }
}
