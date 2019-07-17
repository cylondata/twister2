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
import edu.iu.dsc.tws.api.tset.fn.ReduceFunction;
import edu.iu.dsc.tws.api.tset.ops.ReduceOpFunction;
import edu.iu.dsc.tws.executor.core.OperationNames;

/**
 * Represent a data set create by a all reduce opration
 *
 * @param <T> type of data
 */
public class StreamingAllReduceTLink<T> extends edu.iu.dsc.tws.api.tset.link.BaseTLink<T> {
  private ReduceFunction<T> reduceFn;

  public StreamingAllReduceTLink(TSetEnvironment tSetEnv, ReduceFunction<T> rFn,
                                 int sourceParallelism) {
    super(tSetEnv, TSetUtils.generateName("sallreduce"), sourceParallelism);
    this.reduceFn = rFn;
  }

/*  public <P> StreamingMapTSet<T, P> map(MapFunction<T, P> mapFn) {
    StreamingMapTSet<T, P> set = new StreamingMapTSet<>(getTSetEnv(), mapFn,
        getSourceParallelism());
    addChildToGraph(set);
    return set;
  }

  public <P> StreamingFlatMapTSet<T, P> flatMap(FlatMapFunction<T, P> mapFn) {
    StreamingFlatMapTSet<T, P> set = new StreamingFlatMapTSet<>(getTSetEnv(), mapFn,
        getSourceParallelism());
    addChildToGraph(set);
    return set;
  }*/

/*  @Override
  public void build(TSetGraph tSetGraph) {
//    MessageType dataType = TSetUtils.getDataType(getType());
//
//    connection.allreduce(parent.getName())
//        .viaEdge(Constants.DEFAULT_EDGE)
//        .withReductionFunction(new ReduceOpFunction<>(getReduceFn()))
//        .withDataType(dataType);
  }*/

  @Override
  protected Edge getEdge() {
    return new Edge(getName(), OperationNames.ALLREDUCE, getMessageType(),
        new ReduceOpFunction<>(reduceFn));
  }

  @Override
  public StreamingAllReduceTLink<T> setName(String n) {
    rename(n);
    return this;
  }
}
