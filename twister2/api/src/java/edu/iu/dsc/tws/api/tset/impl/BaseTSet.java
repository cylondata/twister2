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

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.tset.Constants;
import edu.iu.dsc.tws.api.tset.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.MapFunction;
import edu.iu.dsc.tws.api.tset.PartitionFunction;
import edu.iu.dsc.tws.api.tset.ReduceFunction;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.TSet;
import edu.iu.dsc.tws.api.tset.ops.SinkOp;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;

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
  protected int parallel = 4;
  /**
   * The configuration
   */
  private Config config;

  /**
   * If there are sinks
   */
  private List<SinkOp<T>> sinks;

  public BaseTSet(Config cfg, TaskGraphBuilder bldr) {
    this.children = new ArrayList<>();
    this.sinks = new ArrayList<>();
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
  public TSet<T> setName(String n) {
    this.name = n;
    return this;
  }

  @Override
  public <P> TSet<P> map(MapFunction<T, P> mapFn) {
    BaseTSet<P> set = new MapTSet<P, T>(config, builder, this, mapFn);
    children.add(set);
    return set;
  }

  @Override
  public <P> TSet<P> flatMap(FlatMapFunction<T, P> mapFn) {
    BaseTSet<P> set = new FlatMapTSet<P, T>(config, builder, this, mapFn);
    children.add(set);
    return set;
  }

  @Override
  public TSet<T> reduce(ReduceFunction<T> reduceFn) {
    BaseTSet<T> reduce = new ReduceTSet<T>(config, builder, this, reduceFn);
    children.add(reduce);
    return reduce;
  }

  public TSet<T> partition(PartitionFunction<T> partitionFn) {
    return null;
  }

  @Override
  public void sink(Sink<T> sink) {
    sinks.add(new SinkOp<T>(sink));
  }

  @Override
  public void build() {
    // first build our selves
    baseBuild();

    // then build children
    for (BaseTSet<?> c : children) {
      c.build();
    }

    // sinks are a special case
    for (SinkOp<T> sink : sinks) {
      ComputeConnection connection = builder.addSink(getName() + "-sink", sink);
      buildConnection(connection, this);
    }
  }

  public abstract boolean baseBuild();

  /**
   * Get the specific operation name
   * @return operation
   */
  protected abstract Op getOp();

  static <P> void buildConnection(ComputeConnection connection, BaseTSet<P> parent) {
    if (parent.getOp() == Op.REDUCE) {
      ReduceTSet<P> reduceTSet = (ReduceTSet<P>) parent;
      connection.reduce(parent.getName(), Constants.DEFAULT_EDGE,
          new ReduceOpFunction<P>(reduceTSet.getReduceFn()),
          DataType.OBJECT);
    } else if (parent.getOp() == Op.GATHER) {
      connection.gather(parent.getName(), Constants.DEFAULT_EDGE, DataType.OBJECT);
    } else if (parent.getOp() == Op.ALL_REDUCE) {
      AllReduceTSet<P> reduceTSet = (AllReduceTSet<P>) parent;
      connection.allreduce(parent.getName(), Constants.DEFAULT_EDGE,
          new ReduceOpFunction<P>(reduceTSet.getReduceFn()),
          DataType.OBJECT);
    } else if (parent.getOp() == Op.ALL_GATHER) {
      connection.allgather(parent.getName(), Constants.DEFAULT_EDGE, DataType.OBJECT);
    } else if (parent.getOp() == Op.PARTITION) {
      connection.partition(parent.getName(), Constants.DEFAULT_EDGE, DataType.OBJECT);
    } else {
      throw new RuntimeException("Failed to build un-supported operation: " + parent.getOp());
    }
  }
}
