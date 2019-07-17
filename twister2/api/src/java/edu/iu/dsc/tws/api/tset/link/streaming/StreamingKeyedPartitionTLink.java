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

package edu.iu.dsc.tws.api.tset.link.streaming;

import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunction;
import edu.iu.dsc.tws.api.tset.fn.Selector;
import edu.iu.dsc.tws.api.tset.link.KeyValueTLink;

public class StreamingKeyedPartitionTLink<K, V> extends KeyValueTLink<K, V> {

  public StreamingKeyedPartitionTLink(TSetEnvironment tSetEnv, PartitionFunction<K> parFn,
                                      Selector<K, V> selec, int sourceParallelism) {
    super(tSetEnv, TSetUtils.generateName("skpartition"), sourceParallelism, parFn, selec);
  }


/*  public <P> StreamingMapTSet<K, P> map(MapFunction<K, P> mapFn, int parallelism) {
    StreamingMapTSet<K, P> set = new StreamingMapTSet<>(getTSetEnv(), mapFn, parallelism);
    addChildToGraph(set);
    return set;
  }

  public <P> StreamingFlatMapTSet<K, P> flatMap(FlatMapFunction<K, P> mapFn, int parallelism) {
    StreamingFlatMapTSet<K, P> set = new StreamingFlatMapTSet<>(getTSetEnv(), mapFn, parallelism);
    addChildToGraph(set);
    return set;
  }*/

/*  @Override
  public void build(TSetGraph tSetGraph) {
//    MessageType keyType = TSetUtils.getDataType(getClassK());
//    MessageType dataType = TSetUtils.getDataType(getClassT());
//    connection.keyedPartition(parent.getName())
//        .viaEdge(Constants.DEFAULT_EDGE)
//        .withKeyType(keyType).withDataType(dataType)
//        .withTaskPartitioner(new TaskPartitionFunction<>(partitionFunction));
  }*/

  @Override
  protected Edge getEdge() {
    return null;
  }

  @Override
  public StreamingKeyedPartitionTLink<K, V> setName(String n) {
    rename(n);
    return this;
  }
}
