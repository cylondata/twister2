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

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.IterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.fn.NestedIterableMapFunction;
import edu.iu.dsc.tws.api.tset.link.DirectTLink;
import edu.iu.dsc.tws.api.tset.link.KeyValueTLink;
import edu.iu.dsc.tws.api.tset.ops.NestedIterableMapOp;
import edu.iu.dsc.tws.common.config.Config;

/**
 * This is the Map Tset for keyed iterable functions
 *
 * @param <K> input key type
 * @param <V> inout value type
 * @param <O> return type
 */
public class NestedIterableMapTSet<K, V, O> extends BatchBaseTSet<O> {
  private KeyValueTLink<K, V> parent;

  private NestedIterableMapFunction<K, V, O> mapFn;

  public NestedIterableMapTSet(Config cfg, TSetEnv tSetEnv, KeyValueTLink<K, V> parent,
                               NestedIterableMapFunction<K, V, O> mapFunc) {
    super(cfg, tSetEnv);
    this.parent = parent;
    this.mapFn = mapFunc;
    this.parallel = 1;
    this.name = "imap-" + parent.getName();
  }

  public NestedIterableMapTSet(Config cfg, TSetEnv tSetEnv, KeyValueTLink<K, V> parent,
                               NestedIterableMapFunction<K, V, O> mapFunc, int parallelism) {
    super(cfg, tSetEnv);
    this.parent = parent;
    this.mapFn = mapFunc;
    this.parallel = parallelism;
    this.name = "imap-" + parent.getName();
  }

  public <O1> IterableMapTSet<O, O1> map(IterableMapFunction<O, O1> mFn) {
    DirectTLink<O> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    return direct.map(mFn);
  }

  public <O1> IterableFlatMapTSet<O1, O> flatMap(IterableFlatMapFunction<O, O1> mFn) {
    DirectTLink<O> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    return direct.flatMap(mFn);
  }

  public SinkTSet<O> sink(Sink<O> sink) {
    DirectTLink<O> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    return direct.sink(sink);
  }

  @SuppressWarnings("unchecked")
  public boolean baseBuild() {
    boolean isIterable = TSetUtils.isIterableInput(parent, tSetEnv.getTSetBuilder().getOpMode());
    boolean keyed = TSetUtils.isKeyedInput(parent);
    int p = calculateParallelism(parent);
    ComputeConnection connection = tSetEnv.getTSetBuilder().getTaskGraphBuilder().
        addCompute(generateName("i-map",
            parent), new NestedIterableMapOp<>(mapFn, isIterable, keyed), p);
    parent.buildConnection(connection);
    return true;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
    throw new IllegalStateException("Build connections should not be called on a TSet");
  }

  @Override
  public NestedIterableMapTSet<K, V, O> setName(String n) {
    this.name = n;
    return this;
  }
}
