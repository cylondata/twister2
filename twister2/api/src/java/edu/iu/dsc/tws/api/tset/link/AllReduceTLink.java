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
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunction;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunction;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.MapFunction;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunction;
import edu.iu.dsc.tws.api.tset.fn.Sink;
import edu.iu.dsc.tws.api.tset.sets.ComputeCollectorTSet;
import edu.iu.dsc.tws.api.tset.sets.ComputeTSet;
import edu.iu.dsc.tws.api.tset.sets.FlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.MapTSet;
import edu.iu.dsc.tws.api.tset.sets.SinkTSet;

/**
 * Represent a data set create by a all reduce opration
 *
 * @param <T> type of data
 */
public class AllReduceTLink<T> extends edu.iu.dsc.tws.api.tset.link.BaseTLink<T> {
  private ReduceFunction<T> reduceFn;

  public AllReduceTLink(TSetEnvironment tSetEnv, ReduceFunction<T> rFn, int sourceParallelism) {
    super(tSetEnv, TSetUtils.generateName("allreduce"), sourceParallelism);
    this.reduceFn = rFn;
  }

  public <P> MapTSet<T, P> map(MapFunction<T, P> mapFn) {
    MapTSet<T, P> set = new MapTSet<>(getTSetEnv(), mapFn, getTargetParallelism());
    addChildToGraph(set);
    return set;
  }

  public <P> FlatMapTSet<T, P> flatMap(FlatMapFunction<T, P> mapFn) {
    FlatMapTSet<T, P> set = new FlatMapTSet<>(getTSetEnv(), mapFn, getTargetParallelism());
    addChildToGraph(set);
    return set;
  }

  public <P> ComputeTSet<T, P> compute(ComputeFunction<T, P> computeFunction) {
    ComputeTSet<T, P> set = new ComputeTSet<>(getTSetEnv(), computeFunction,
        getTargetParallelism());
    addChildToGraph(set);
    return set;
  }

  public <P> ComputeCollectorTSet<T, P> compute(ComputeCollectorFunction<T, P>
                                                    computeFunction) {
    ComputeCollectorTSet<T, P> set = new ComputeCollectorTSet<>(getTSetEnv(),
        computeFunction, getTargetParallelism());
    addChildToGraph(set);
    return set;
  }

  @Override
  public void build(TSetGraph tSetGraph) {
//    MessageType dataType = TSetUtils.getDataType(getType());
//
//    connection.allreduce(parent.getName())
//        .viaEdge(Constants.DEFAULT_EDGE)
//        .withReductionFunction(new ReduceOpFunction<T>(getReduceFn()))
//        .withDataType(dataType);
  }

  public ReduceFunction<T> getReduceFn() {
    return reduceFn;
  }

  @Override
  public AllReduceTLink<T> setName(String n) {
    rename(n);
    return this;
  }
}
