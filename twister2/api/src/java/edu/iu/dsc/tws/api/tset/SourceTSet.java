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
package edu.iu.dsc.tws.api.tset;

import java.util.Random;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.link.DirectTLink;
import edu.iu.dsc.tws.api.tset.ops.SourceOp;
import edu.iu.dsc.tws.common.config.Config;

public class SourceTSet<T> extends BaseTSet<T> {
  private Source<T> source;

  public SourceTSet(Config cfg, TSetBuilder bldr, Source<T> src) {
    super(cfg, bldr);
    this.source = src;
    this.name = "source-" + new Random(System.nanoTime()).nextInt(10);
  }

  public <P> MapTSet<P, T> map(MapFunction<T, P> mapFn) {
    DirectTLink<T> direct = new DirectTLink<>(config, builder, this);
    children.add(direct);
    MapTSet<P, T> set = new MapTSet<P, T>(config, builder, direct, mapFn);
    children.add(set);
    return set;
  }

  public <P> FlatMapTSet<P, T> flatMap(FlatMapFunction<T, P> mapFn) {
    DirectTLink<T> direct = new DirectTLink<>(config, builder, this);
    children.add(direct);
    FlatMapTSet<P, T> set = new FlatMapTSet<P, T>(config, builder, direct, mapFn);
    children.add(set);
    return set;
  }

  public <P> IMapTSet<P, T> map(IterableMapFunction<T, P> mapFn) {
    DirectTLink<T> direct = new DirectTLink<>(config, builder, this);
    children.add(direct);
    IMapTSet<P, T> set = new IMapTSet<>(config, builder, direct, mapFn);
    children.add(set);
    return set;
  }

  public <P> IFlatMapTSet<P, T> flatMap(IterableFlatMapFunction<T, P> mapFn) {
    DirectTLink<T> direct = new DirectTLink<>(config, builder, this);
    children.add(direct);
    IFlatMapTSet<P, T> set = new IFlatMapTSet<>(config, builder, direct, mapFn);
    children.add(set);
    return set;
  }

  public SinkTSet<T> sink(Sink<T> sink) {
    DirectTLink<T> direct = new DirectTLink<>(config, builder, this);
    children.add(direct);
    SinkTSet<T> sinkTSet = new SinkTSet<>(config, builder, direct, sink);
    children.add(sinkTSet);
    return sinkTSet;
  }

  @Override
  public boolean baseBuild() {
    builder.getTaskGraphBuilder().addSource(getName(), new SourceOp<T>(source), parallel);
    return true;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
  }
}
