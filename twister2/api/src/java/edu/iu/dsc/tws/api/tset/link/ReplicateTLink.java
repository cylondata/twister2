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

import java.util.Iterator;

import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetGraph;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunction;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunction;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.MapFunction;
import edu.iu.dsc.tws.api.tset.sets.ComputeCollectorTSet;
import edu.iu.dsc.tws.api.tset.sets.ComputeTSet;
import edu.iu.dsc.tws.api.tset.sets.FlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.MapTSet;

public class ReplicateTLink<T> extends BaseTLink<T> {

  public ReplicateTLink(TSetEnvironment tSetEnv, int reps) {
    super(tSetEnv, TSetUtils.generateName("replicate"), 1, reps);
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

  public <P> ComputeTSet<Iterator<T>, P> compute(ComputeFunction<Iterator<T>, P> computeFunction) {
    ComputeTSet<Iterator<T>, P> set = new ComputeTSet<>(getTSetEnv(), computeFunction,
        getTargetParallelism());
    addChildToGraph(set);
    return set;
  }

  public <P> ComputeCollectorTSet<Iterator<T>, P> compute(ComputeCollectorFunction<Iterator<T>, P>
                                                              computeFunction) {
    ComputeCollectorTSet<Iterator<T>, P> set = new ComputeCollectorTSet<>(getTSetEnv(),
        computeFunction, getTargetParallelism());
    addChildToGraph(set);
    return set;
  }

  @Override
  public void build(TSetGraph tSetGraph) {
//    MessageType dataType = TSetUtils.getDataType(getType());
//
//    connection.broadcast(parent.getName())
//        .viaEdge(Constants.DEFAULT_EDGE).withDataType(dataType).connect();
  }

  @Override
  public ReplicateTLink<T> setName(String n) {
    rename(n);
    return this;
  }
}
