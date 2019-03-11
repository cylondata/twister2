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

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.IterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.MapFunction;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.link.BaseTLink;
import edu.iu.dsc.tws.api.tset.link.DirectTLink;
import edu.iu.dsc.tws.api.tset.ops.FlatMapOp;
import edu.iu.dsc.tws.common.config.Config;

/**
 * Apply a flat map operation
 *
 * @param <T> the input type
 * @param <P> the output type
 */
public class FlatMapTSet<T, P> extends BaseTSet<T> {
  private static final Logger LOG = Logger.getLogger(FlatMapTSet.class.getName());

  private BaseTLink<P> parent;

  private FlatMapFunction<P, T> mapFn;

  public FlatMapTSet(Config cfg, TSetEnv tSetEnv, BaseTLink<P> parent,
                     FlatMapFunction<P, T> mapFunc) {
    super(cfg, tSetEnv);
    this.parent = parent;
    this.mapFn = mapFunc;
  }

  public <P1> MapTSet<P1, T> map(MapFunction<T, P1> mFn) {
    DirectTLink<T> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    MapTSet<P1, T> set = new MapTSet<P1, T>(config, tSetEnv, direct, mFn);
    children.add(set);
    return set;
  }

  public <P1> FlatMapTSet<P1, T> flatMap(FlatMapFunction<T, P1> mFn) {
    DirectTLink<T> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    FlatMapTSet<P1, T> set = new FlatMapTSet<P1, T>(config, tSetEnv, direct, mFn);
    children.add(set);
    return set;
  }

  public <P1> IMapTSet<P1, T> map(IterableMapFunction<T, P1> mFn) {
    DirectTLink<T> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    IMapTSet<P1, T> set = new IMapTSet<>(config, tSetEnv, direct, mFn);
    children.add(set);
    return set;
  }

  public <P1> IFlatMapTSet<P1, T> flatMap(IterableFlatMapFunction<T, P1> mFn) {
    DirectTLink<T> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    IFlatMapTSet<P1, T> set = new IFlatMapTSet<>(config, tSetEnv, direct, mFn);
    children.add(set);
    return set;
  }

  public SinkTSet<T> sink(Sink<T> sink) {
    DirectTLink<T> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    SinkTSet<T> sinkTSet = new SinkTSet<>(config, tSetEnv, direct, sink);
    children.add(sinkTSet);
    tSetEnv.run();
    return sinkTSet;
  }

  @SuppressWarnings("unchecked")
  public boolean baseBuild() {
    boolean isIterable = TSetUtils.isIterableInput(parent, tSetEnv.getTSetBuilder().getOpMode());
    boolean keyed = TSetUtils.isKeyedInput(parent);

    int p = calculateParallelism(parent);
    String newName = generateName("flat-map", parent);
    mapFn.addInputs(inputMap);
    ComputeConnection connection = tSetEnv.getTSetBuilder().getTaskGraphBuilder().
        addCompute(newName, new FlatMapOp<>(mapFn, isIterable, keyed), p);
    parent.buildConnection(connection);
    return true;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
  }
}
