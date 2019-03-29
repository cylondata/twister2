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

package edu.iu.dsc.tws.api.tset.sets.streaming;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.MapFunction;
import edu.iu.dsc.tws.api.tset.link.BaseTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingDirectTLink;
import edu.iu.dsc.tws.api.tset.ops.FlatMapOp;
import edu.iu.dsc.tws.api.tset.sets.SinkTSet;
import edu.iu.dsc.tws.common.config.Config;

/**
 * Apply a flat map operation
 *
 * @param <T> the input type
 * @param <P> the output type
 */
public class StreamingFlatMapTSet<T, P> extends StreamingBaseTSet<T> {
  private static final Logger LOG = Logger.getLogger(StreamingFlatMapTSet.class.getName());

  private BaseTLink<P> parent;

  private FlatMapFunction<P, T> mapFn;

  public StreamingFlatMapTSet(Config cfg, TSetEnv tSetEnv, BaseTLink<P> parent,
                              FlatMapFunction<P, T> mapFunc) {
    super(cfg, tSetEnv);
    this.parent = parent;
    this.mapFn = mapFunc;
    this.parallel = 1;
  }

  public StreamingFlatMapTSet(Config cfg, TSetEnv tSetEnv, BaseTLink<P> parent,
                              FlatMapFunction<P, T> mapFunc, int parallelism) {
    super(cfg, tSetEnv);
    this.parent = parent;
    this.mapFn = mapFunc;
    this.parallel = parallelism;
  }

  public <P1> StreamingMapTSet<P1, T> map(MapFunction<T, P1> mFn) {
    StreamingDirectTLink<T> direct = new StreamingDirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    return direct.map(mFn);
  }

  public <P1> StreamingFlatMapTSet<P1, T> flatMap(FlatMapFunction<T, P1> mFn) {
    StreamingDirectTLink<T> direct = new StreamingDirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    return direct.flatMap(mFn);
  }


  public SinkTSet<T> sink(Sink<T> sink) {
    StreamingDirectTLink<T> direct = new StreamingDirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    return direct.sink(sink);
  }

  @SuppressWarnings("unchecked")
  public boolean baseBuild() {
    boolean isIterable = TSetUtils.isIterableInput(parent, tSetEnv.getTSetBuilder().getOpMode());
    boolean keyed = TSetUtils.isKeyedInput(parent);

    int p = calculateParallelism(parent);
    String newName = generateName("flat-map", parent);
    ComputeConnection connection = tSetEnv.getTSetBuilder().getTaskGraphBuilder().
        addCompute(newName, new FlatMapOp<>(mapFn, isIterable, keyed), p);
    parent.buildConnection(connection);
    return true;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
    throw new IllegalStateException("Build connections should not be called on a TSet");
  }

  @Override
  public StreamingFlatMapTSet<T, P> setName(String n) {
    this.name = n;
    return this;
  }
}
