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
package edu.iu.dsc.tws.api.tset.impl;

import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.tset.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.MapFunction;
import edu.iu.dsc.tws.api.tset.PartitionFunction;
import edu.iu.dsc.tws.api.tset.ReduceFunction;
import edu.iu.dsc.tws.api.tset.TSet;
import edu.iu.dsc.tws.common.config.Config;

public abstract class BaseTSet<T> implements TSet<T> {
  /**
   * The children of this set
   */
  protected List<BaseTSet<?>> children;

  /**
   * The builder to use to building the task graph
   */
  protected TaskGraphBuilder builder;

  /**
   * Name of the data set
   */
  protected String name;

  /**
   * The parallelism of the set
   */
  protected int parallel;

  /**
   * The configuration
   */
  private Config config;

  public BaseTSet(Config cfg, TaskGraphBuilder bldr) {
    this.children = new ArrayList<>();
    this.builder = bldr;
    this.config = cfg;
  }

  public String getName() {
    return name;
  }

  public int getParallelism() {
    return parallel;
  }

  @Override
  public <P> TSet<P> map(MapFunction<T, P> mapFn) {
    return new MapTSet<P, T>(config, builder, this, mapFn);
  }

  @Override
  public <P> TSet<P> flatMap(FlatMapFunction<T, P> mapFn) {
    return new FlatMapTSet<P, T>(config, builder, this, mapFn);
  }

  @Override
  public TSet<T> reduce(ReduceFunction<T> reduceFn) {
    return null;
  }

  public TSet<T> partition(PartitionFunction<T> partitionFn) {
    return null;
  }

  @Override
  public void build() {
    for (BaseTSet<?> c : children) {
      c.build();
    }
  }
}
