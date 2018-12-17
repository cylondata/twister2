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

import java.util.ArrayList;
import java.util.List;

import com.google.common.reflect.TypeToken;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
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
  protected Config config;

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
  public <P> MapTSet<P, T> map(MapFunction<T, P> mapFn) {
    MapTSet<P, T> set = new MapTSet<P, T>(config, builder, this, mapFn);
    children.add(set);
    return set;
  }

  @Override
  public <P> FlatMapTSet<P, T> flatMap(FlatMapFunction<T, P> mapFn) {
    FlatMapTSet<P, T> set = new FlatMapTSet<P, T>(config, builder, this, mapFn);
    children.add(set);
    return set;
  }

  @Override
  public <P> IMapTSet<P, T> map(IterableMapFunction<T, P> mapFn) {
    IMapTSet<P, T> set = new IMapTSet<>(config, builder, this, mapFn);
    children.add(set);
    return set;
  }

  @Override
  public <P> IFlatMapTSet<P, T> flatMap(IterableFlatMapFunction<T, P> mapFn) {
    IFlatMapTSet<P, T> set = new IFlatMapTSet<>(config, builder, this, mapFn);
    children.add(set);
    return set;
  }

  @Override
  public ReduceTSet<T> reduce(ReduceFunction<T> reduceFn) {
    ReduceTSet<T> reduce = new ReduceTSet<T>(config, builder, this, reduceFn);
    children.add(reduce);
    return reduce;
  }

  public PartitionTSet<T> partition(PartitionFunction<T> partitionFn) {
    PartitionTSet<T> partition = new PartitionTSet<>(config, builder, this, partitionFn);
    children.add(partition);
    return partition;
  }

  @Override
  public GatherTSet<T> gather() {
    GatherTSet<T> gather = new GatherTSet<>(config, builder, this);
    children.add(gather);
    return gather;
  }

  @Override
  public TSet<T> allReduce(ReduceFunction<T> reduceFn) {
    BaseTSet<T> reduce = new AllReduceTSet<T>(config, builder, this, reduceFn);
    children.add(reduce);
    return reduce;
  }

  @Override
  public TSet<T> allGather() {
    BaseTSet<T> gather = new AllGatherTSet<>(config, builder, this);
    children.add(gather);
    return gather;
  }

  @Override
  public <K> GroupedTSet<T, K> groupBy(PartitionFunction<K> partitionFunction,
                                       Selector<T, K> selector) {
    GroupedTSet<T, K> groupedTSet = new GroupedTSet<>(config, builder, this,
        partitionFunction, selector);
    children.add(groupedTSet);
    return groupedTSet;
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
      // call build connection of our selves
      buildConnection(connection);
    }
  }

  public abstract boolean baseBuild();

  static <T> boolean isIterableInput(BaseTSet<T> parent) {
    if (parent instanceof ReduceTSet || parent instanceof KeyedReduceTSet) {
      return false;
    } else if (parent instanceof GatherTSet || parent instanceof KeyedGatherTSet) {
      return true;
    } else if (parent instanceof AllReduceTSet) {
      return false;
    } else if (parent instanceof AllGatherTSet) {
      return true;
    } else if (parent instanceof PartitionTSet || parent instanceof KeyedPartitionTSet) {
      return true;
    } else {
      throw new RuntimeException("Failed to build un-supported operation: " + parent);
    }
  }

  protected static DataType getDataType(Class type) {
    if (type == int[].class) {
      return DataType.INTEGER;
    } else if (type == double[].class) {
      return DataType.DOUBLE;
    } else if (type == short[].class) {
      return DataType.SHORT;
    } else if (type == byte[].class) {
      return DataType.BYTE;
    } else if (type == long[].class) {
      return DataType.LONG;
    } else if (type == char[].class) {
      return DataType.CHAR;
    } else {
      return DataType.OBJECT;
    }
  }

  protected static DataType getKeyType(Class type) {
    if (type == Integer.class) {
      return DataType.INTEGER;
    } else if (type == Double.class) {
      return DataType.DOUBLE;
    } else if (type == Short.class) {
      return DataType.SHORT;
    } else if (type == Byte.class) {
      return DataType.BYTE;
    } else if (type == Long.class) {
      return DataType.LONG;
    } else if (type == Character.class) {
      return DataType.CHAR;
    } else {
      return DataType.OBJECT;
    }
  }

  protected Class getType() {
    TypeToken<T> typeToken = new TypeToken<T>(getClass()) {
    };
    return typeToken.getRawType();
  }

  abstract void buildConnection(ComputeConnection connection);
}
