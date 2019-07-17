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

import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetGraph;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.IterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.fn.NestedIterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.Sink;
import edu.iu.dsc.tws.api.tset.link.DirectTLink;

public class NestedIterableFlatMapTSet<K, V, O> extends BatchBaseTSet<O> {

  private NestedIterableFlatMapFunction<K, V, O> mapFn;

  public NestedIterableFlatMapTSet(TSetEnvironment tSetEnv,
                                   NestedIterableFlatMapFunction<K, V, O> mapFunc,
                                   int parallelism) {
    super(tSetEnv, TSetUtils.generateName("niflatmap"), parallelism);
    this.mapFn = mapFunc;
  }

  public <O1> IterableMapTSet<O, O1> map(IterableMapFunction<O, O1> mFn) {
    DirectTLink<O> direct = new DirectTLink<>(getTSetEnv(), getParallelism());
    addChildToGraph(direct);
    return direct.map(mFn);
  }

  public <O1> IterableFlatMapTSet<O, O1> flatMap(IterableFlatMapFunction<O, O1> mFn) {
    DirectTLink<O> direct = new DirectTLink<>(getTSetEnv(), getParallelism());
    addChildToGraph(direct);
    return direct.flatMap(mFn);
  }

  public SinkTSet<O> sink(Sink<O> sink) {
//    DirectTLink<O> direct = new DirectTLink<>(config, tSetEnv, this);
//    addChildToGraph(direct);
//    return direct.sink(sink);
    return null;
  }

  @Override
  public void build(TSetGraph tSetGraph) {
//    boolean isIterable = TSetUtils.isIterableInput(parent, tSetEnv.getTSetBuilder().getOpMode());
//    boolean keyed = TSetUtils.isKeyedInput(parent);
//
//    // lets override the parallelism
//    int p = calculateParallelism(parent);
//    ComputeConnection connection = tSetEnv.getTSetBuilder().getTaskGraphBuilder().
//        addCompute(generateName("i-flat-map", parent),
//            new NestedIterableFlatMapOp<>(mapFn, isIterable, keyed), p);
//    parent.buildConnection(connection);
//    return true;
  }

  @Override
  public NestedIterableFlatMapTSet<K, V, O> setName(String n) {
    rename(n);
    return this;
  }
}

