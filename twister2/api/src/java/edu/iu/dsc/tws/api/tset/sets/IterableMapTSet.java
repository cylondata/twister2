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
import edu.iu.dsc.tws.api.tset.fn.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.fn.Sink;
import edu.iu.dsc.tws.api.tset.link.DirectTLink;
import edu.iu.dsc.tws.api.tset.ops.IterableMapOp;

/**
 * Iterable Map Set
 *
 * @param <I> Input type
 * @param <O> Output type
 */
public class IterableMapTSet<I, O> extends BatchBaseTSet<O> {
  private IterableMapFunction<I, O> mapFn;

  public IterableMapTSet(TSetEnvironment tSetEnv, IterableMapFunction<I, O> mapFunc,
                         int parallelism) {
    super(tSetEnv, TSetUtils.generateName("imap"), parallelism);
    this.mapFn = mapFunc;
  }

  /**
   * The Input of the mFn is the output of the current tSet
   */
/*
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
*/
  public SinkTSet<O> sink(Sink<O> sink) {
    DirectTLink<O> direct = new DirectTLink<>(getTSetEnv(), getParallelism());
    addChildToGraph(direct);
    return direct.sink(sink);
  }

/*  @Override
  public void build(TSetGraph tSetGraph) {
    boolean isIterable = TSetUtils.isIterableInput(parent, tSetEnv.getTSetBuilder().getOpMode());
    boolean keyed = TSetUtils.isKeyedInput(parent);

    getTSetEnv().getGraph().getDfwGraphBuilder().addTask(getName(),
        new IterableMapOp<>(mapFn, isIterable, keyed), getParallelism());
  }*/

  @Override
  protected ICompute getTask() {
    // todo: fix this
    return new IterableMapOp<>(mapFn, false, false);
  }

  @Override
  public IterableMapTSet<I, O> setName(String name) {
    rename(name);
    return this;
  }
}
