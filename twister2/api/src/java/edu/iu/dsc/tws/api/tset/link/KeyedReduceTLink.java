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
import edu.iu.dsc.tws.api.tset.fn.KIterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.KIterableMapFunction;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunction;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunction;
import edu.iu.dsc.tws.api.tset.fn.Selector;
import edu.iu.dsc.tws.api.tset.sets.KIterableFlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.KIterableMapTSet;

public class KeyedReduceTLink<K, V> extends KeyValueTLink<K, V> {
  private ReduceFunction<V> reduceFn;

  public KeyedReduceTLink(TSetEnvironment tSetEnv, ReduceFunction<V> rFn,
                          PartitionFunction<K> parFn, Selector<K, V> selec, int sourceParallelism) {
    super(tSetEnv, TSetUtils.generateName("kreduce"), sourceParallelism, parFn, selec);
    this.reduceFn = rFn;
  }

  public <O> KIterableMapTSet<K, V, O> map(KIterableMapFunction<K, V, O> mapFn) {
    KIterableMapTSet<K, V, O> set = new KIterableMapTSet<>(getTSetEnv(), ,
        mapFn, getSourceParallelism());
    addChildToGraph(set);
    return set;
  }

  public <O> KIterableFlatMapTSet<K, V, O> flatMap(KIterableFlatMapFunction<K, V, O> mapFn) {
    KIterableFlatMapTSet<K, V, O> set = new KIterableFlatMapTSet<>(getTSetEnv(), getSelector(),
        mapFn, getSourceParallelism());
    addChildToGraph(set);
    return set;
  }

  public ReduceFunction<V> getReduceFn() {
    return reduceFn;
  }

  @Override
  public void build(TSetGraph tSetGraph) {
//    MessageType keyType = TSetUtils.getDataType(getClassK());
//    MessageType dataType = TSetUtils.getDataType(getClassV());
//    connection.keyedReduce(parent.getName())
//        .viaEdge(Constants.DEFAULT_EDGE)
//        .withReductionFunction(new ReduceOpFunction<>(reduceFn))
//        .withKeyType(keyType).withDataType(dataType)
//        .withTaskPartitioner(new TaskPartitionFunction<>(partitionFunction));
  }

  @Override
  public KeyedReduceTLink<K, V> setName(String n) {
    rename(n);
    return this;
  }

}
