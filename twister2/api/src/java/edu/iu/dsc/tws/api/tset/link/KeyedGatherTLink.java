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

package edu.iu.dsc.tws.api.tset.link;

import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetGraph;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.NestedIterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.NestedIterableMapFunction;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunction;
import edu.iu.dsc.tws.api.tset.fn.Selector;
import edu.iu.dsc.tws.api.tset.sets.NestedIterableFlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.NestedIterableMapTSet;

public class KeyedGatherTLink<K, V> extends KeyValueTLink<K, V> {

  public KeyedGatherTLink(TSetEnvironment tSetEnv, PartitionFunction<K> parFn, Selector<K, V> selc,
                          int sourceParallelism) {
    super(tSetEnv, TSetUtils.generateName("kgather"), sourceParallelism, parFn, selc);
  }

  public <O> NestedIterableMapTSet<K, V, O> map(NestedIterableMapFunction<K, V, O> mapFn) {
    NestedIterableMapTSet<K, V, O> set = new NestedIterableMapTSet<>(getTSetEnv(), mapFn,
        getSourceParallelism());
    addChildToGraph(set);
    return set;
  }

  public <O> NestedIterableFlatMapTSet<K, V, O> flatMap(
      NestedIterableFlatMapFunction<K, V, O> mapFn) {
    NestedIterableFlatMapTSet<K, V, O> set = new NestedIterableFlatMapTSet<>(getTSetEnv(), mapFn,
        getSourceParallelism());
    addChildToGraph(set);
    return set;
  }

  @Override
  public void build(TSetGraph tSetGraph) {
//    MessageType keyType = TSetUtils.getDataType(getClassK());
//    MessageType dataType = TSetUtils.getDataType(getClassV());
//    connection.keyedGather(parent.getName())
//        .viaEdge(Constants.DEFAULT_EDGE)
//        .withKeyType(keyType)
//        .withDataType(dataType)
//        .withTaskPartitioner(new TaskPartitionFunction<>(partitionFunction));
  }

  @Override
  public KeyedGatherTLink<K, V> setName(String n) {
    rename(n);
    return this;
  }
}
