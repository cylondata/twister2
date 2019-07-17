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
import edu.iu.dsc.tws.api.tset.link.BaseTLink;
import edu.iu.dsc.tws.api.tset.ops.TaskPartitionFunction;
import edu.iu.dsc.tws.executor.core.OperationNames;

public class StreamingPartitionTLink<T> extends BaseTLink<T> {

  private PartitionFunction<T> partitionFunction;

  public StreamingPartitionTLink(TSetEnvironment tSetEnv, int sourceParallelism) {
    this(tSetEnv, null, sourceParallelism);
  }

  public StreamingPartitionTLink(TSetEnvironment tSetEnv, PartitionFunction<T> parFn,
                                 int sourceParallelism) {
    super(tSetEnv, TSetUtils.generateName("spartition"), sourceParallelism);
    this.partitionFunction = parFn;
  }

/*  public <P> MapTSet<T, P> map(MapFunction<T, P> mapFn) {
    MapTSet<T, P> set = new MapTSet<>(getTSetEnv(), mapFn, getSourceParallelism());
    addChildToGraph(set);
    return set;
  }

  public <P> FlatMapTSet<T, P> flatMap(FlatMapFunction<T, P> mapFn) {
    FlatMapTSet<T, P> set = new FlatMapTSet<>(getTSetEnv(), mapFn, getSourceParallelism());
    addChildToGraph(set);
    return set;
  }*/

/*  @Override
  public void build(TSetGraph tSetGraph) {
//    MessageType dataType = TSetUtils.getDataType(getType());
//
//    connection.partition(parent.getName()).viaEdge(Constants.DEFAULT_EDGE).withDataType(dataType);
  }*/

  @Override
  protected Edge getEdge() {
    Edge e = new Edge(getName(), OperationNames.PARTITION, getMessageType());
    if (partitionFunction != null) {
      e.setPartitioner(new TaskPartitionFunction<>(partitionFunction));
    }
    return e;
  }

  @Override
  public StreamingPartitionTLink<T> setName(String n) {
    rename(n);
    return this;
  }
}
