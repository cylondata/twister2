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
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.ops.FlatMapOp;

/**
 * Apply a flat map operation
 * todo: fix input output direction!!!
 *
 * @param <T> the input type
 * @param <P> the output type
 */
public class FlatMapTSet<T, P> extends BatchBaseTSet<P> {
  private FlatMapFunction<T, P> mapFn;

  public FlatMapTSet(TSetEnvironment tSetEnv, FlatMapFunction<T, P> mapFunc, int parallelism) {
    super(tSetEnv, TSetUtils.generateName("fmap"), parallelism);
    this.mapFn = mapFunc;
  }

/*  public SinkTSet<T> sink(Sink<T> sink) {
    DirectTLink<T> direct = new DirectTLink<>(getTSetEnv(), getParallelism());
    addChildToGraph(direct);
    return direct.sink(sink);
  }*/

/*
  @Override
  public void build(TSetGraph tSetGraph) {
    boolean isIterable = TSetUtils.isIterableInput(parent, tSetEnv.getTSetBuilder().getOpMode());
    boolean keyed = TSetUtils.isKeyedInput(parent);

    int p = calculateParallelism(parent);
    String newName = generateName("flat-map", parent);
    ComputeConnection connection = tSetEnv.getTSetBuilder().getTaskGraphBuilder().
        addCompute(newName, new FlatMapOp<>(mapFn, isIterable, keyed), p);
    parent.buildConnection(connection);
  }
*/

  @Override
  public FlatMapTSet<T, P> setName(String n) {
    rename(n);
    return this;
  }


  @Override
  protected ICompute getTask() {
    return new FlatMapOp<>(mapFn, false, false);
  }
}
