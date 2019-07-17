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

import edu.iu.dsc.tws.api.task.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunction;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunction;
import edu.iu.dsc.tws.api.tset.fn.Selector;
import edu.iu.dsc.tws.api.tset.link.KeyedGatherTLink;
import edu.iu.dsc.tws.api.tset.link.KeyedPartitionTLink;
import edu.iu.dsc.tws.api.tset.link.KeyedReduceTLink;

/**
 * TODO: FIX THIS!!!! there is an issue with the build, because this tset does not create a
 * icompute. figure out a way to handle the connection between the previous link and the the
 * next keyed link
 * <p>
 * Attaches a key to the oncoming data.
 * NOTE: does not create a physical icompute task, but rather holds the partitioner and selector
 * information for the downstream keyed tsets
 *
 * @param <K> key type
 * @param <V> data (value) type
 */
public class GroupedTSet<K, V> extends BatchBaseTSet<V> {
  private PartitionFunction<K> partitioner;

  private Selector<K, V> selector;

  /*
  Since this tset does not create a icompute task, it should inherit the name of the source.
  this would make sure that getName() method returns the name of the source, and hence creating.
   */
  public GroupedTSet(TSetEnvironment tSetEnv, PartitionFunction<K> partFn, Selector<K, V> selc,
                     int parallelism) {
    super(tSetEnv, TSetUtils.generateName("groupby"), parallelism);
    this.partitioner = partFn;
    this.selector = selc;
  }

  public KeyedReduceTLink<K, V> keyedReduce(ReduceFunction<V> reduceFn) {
    KeyedReduceTLink<K, V> reduce = new KeyedReduceTLink<>(getTSetEnv(), reduceFn, partitioner,
        selector, getParallelism());
    addChildToGraph(reduce);
    return reduce;
  }

  public KeyedPartitionTLink<K, V> keyedPartition() {
    KeyedPartitionTLink<K, V> partition = new KeyedPartitionTLink<>(getTSetEnv(), partitioner,
        selector, getParallelism());
    addChildToGraph(partition);
    return partition;
  }

  public KeyedGatherTLink<K, V> keyedGather() {
    KeyedGatherTLink<K, V> gather = new KeyedGatherTLink<>(getTSetEnv(), partitioner,
        selector, getParallelism());
    addChildToGraph(gather);
    return gather;
  }


  @Override
  public GroupedTSet<K, V> setName(String n) {
    rename(n);
    return this;
  }

/*
  @Override
  public void build(TSetGraph tSetGraph) {
    // nothing to build here. There will be no task created by a grouped tset
  }
*/

  @Override
  protected ICompute getTask() {
    throw new UnsupportedOperationException("group tset does not create any tasks!");
  }
}
