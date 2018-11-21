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
import edu.iu.dsc.tws.api.tset.ReduceFunction;
import edu.iu.dsc.tws.api.tset.TSet;

public abstract class BaseTSet<T> implements TSet<T> {
  private List<BaseTSet<?>> children;

  /**
   * The builder to use to building the task graph
   */
  private TaskGraphBuilder builder;

  public BaseTSet() {
    this.children = new ArrayList<>();
  }

  @Override
  public <P> TSet<P> map(MapFunction<T, ? extends P> mapFn) {
    return null;
  }

  @Override
  public <P> TSet<P> flatMap(FlatMapFunction<T, ? extends P> mapFn) {
    return null;
  }

  @Override
  public TSet<T> reduce(ReduceFunction<T> reduceFn) {
    return null;
  }
}
