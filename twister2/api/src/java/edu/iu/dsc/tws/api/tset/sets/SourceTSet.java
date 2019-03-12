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

import java.util.Random;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.IterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.MapFunction;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.Source;
import edu.iu.dsc.tws.api.tset.TSet;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.link.DirectTLink;
import edu.iu.dsc.tws.api.tset.ops.SourceOp;
import edu.iu.dsc.tws.common.config.Config;

public class SourceTSet<T> extends BaseTSet<T> {
  private Source<T> source;

  public SourceTSet(Config cfg, TSetEnv tSetEnv, Source<T> src) {
    super(cfg, tSetEnv);
    this.source = src;
    this.name = "source-" + new Random(System.nanoTime()).nextInt(10);
  }

  public <P> MapTSet<P, T> map(MapFunction<T, P> mapFn) {
    DirectTLink<T> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    MapTSet<P, T> set = new MapTSet<P, T>(config, tSetEnv, direct, mapFn);
    children.add(set);
    return set;
  }

  public <P> FlatMapTSet<P, T> flatMap(FlatMapFunction<T, P> mapFn) {
    DirectTLink<T> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    FlatMapTSet<P, T> set = new FlatMapTSet<P, T>(config, tSetEnv, direct, mapFn);
    children.add(set);
    return set;
  }

  public <P> IMapTSet<P, T> map(IterableMapFunction<T, P> mapFn) {
    DirectTLink<T> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    IMapTSet<P, T> set = new IMapTSet<>(config, tSetEnv, direct, mapFn);
    children.add(set);
    return set;
  }

  public <P> IFlatMapTSet<P, T> flatMap(IterableFlatMapFunction<T, P> mapFn) {
    DirectTLink<T> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    IFlatMapTSet<P, T> set = new IFlatMapTSet<>(config, tSetEnv, direct, mapFn);
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

  @Override
  public boolean baseBuild() {
    source.addInputs(inputMap);
    tSetEnv.getTSetBuilder().getTaskGraphBuilder().
        addSource(getName(), new SourceOp<T>(source), parallel);
    return true;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
    throw new IllegalStateException("Build connections should not be called on a TSet");
  }

  @Override
  public SourceTSet<T> setParallelism(int parallelism) {
    this.parallel = parallelism;
    return this;
  }

  @Override
  public SourceTSet<T> setName(String n) {
    this.name = n;
    return this;
  }
}
