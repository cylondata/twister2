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

import java.util.ArrayList;
import java.util.List;

import com.google.common.reflect.TypeToken;

import edu.iu.dsc.tws.api.tset.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.IterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.MapFunction;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.sets.FlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.IFlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.IMapTSet;
import edu.iu.dsc.tws.api.tset.sets.MapTSet;
import edu.iu.dsc.tws.api.tset.sets.SinkTSet;
import edu.iu.dsc.tws.common.config.Config;

public abstract class BaseTLink<T> implements TLink<T> {

  /**
   * The children of this set
   */
  protected List<TBase<?>> children;

  /**
   * The TSet Env used for runtime operations
   */
  protected TSetEnv tSetEnv;


  /**
   * Name of the data set
   */
  protected String name;

  /**
   * The parallelism of the set
   */
  protected int parallel = 4;
  /**
   * The configuration
   */
  protected Config config;

  public BaseTLink(Config cfg, TSetEnv tSetEnv) {
    this.children = new ArrayList<>();
    this.tSetEnv = tSetEnv;
    this.config = cfg;
  }

  @Override
  public TLink<T> setName(String n) {
    this.name = n;
    return this;
  }

  public String getName() {
    return name;
  }

  public int getParallelism() {
    return parallel;
  }

  @Override
  public TLink<T> setParallelism(int parallelism) {
    return null;
  }

  @Override
  public <P> MapTSet<P, T> map(MapFunction<T, P> mapFn) {
    MapTSet<P, T> set = new MapTSet<P, T>(config, tSetEnv, this, mapFn);
    children.add(set);
    return set;
  }

  @Override
  public <P> FlatMapTSet<P, T> flatMap(FlatMapFunction<T, P> mapFn) {
    FlatMapTSet<P, T> set = new FlatMapTSet<P, T>(config, tSetEnv, this, mapFn);
    children.add(set);
    return set;
  }

  @Override
  public <P> IMapTSet<P, T> map(IterableMapFunction<T, P> mapFn) {
    IMapTSet<P, T> set = new IMapTSet<>(config, tSetEnv, this, mapFn);
    children.add(set);
    return set;
  }

  @Override
  public <P> IFlatMapTSet<P, T> flatMap(IterableFlatMapFunction<T, P> mapFn) {
    IFlatMapTSet<P, T> set = new IFlatMapTSet<>(config, tSetEnv, this, mapFn);
    children.add(set);
    return set;
  }

  @Override
  public SinkTSet<T> sink(Sink<T> sink) {
    SinkTSet<T> sinkTSet = new SinkTSet<>(config, tSetEnv, this, sink);
    children.add(sinkTSet);
    tSetEnv.run();
    return sinkTSet;
  }

  @Override
  public void build() {
// first build our selves
    baseBuild();

    // then build children
    for (TBase<?> c : children) {
      c.build();
    }
  }

  protected Class getType() {
    TypeToken<T> typeToken = new TypeToken<T>(getClass()) {
    };
    return typeToken.getRawType();
  }

  /**
   * Override the parallelism
   *
   * @return if overide, return value, otherwise -1
   */
  public int overrideParallelism() {
    return -1;
  }

  public List<TBase<?>> getChildren() {
    return children;
  }

}
