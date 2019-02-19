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
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.reflect.TypeToken;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.task.graph.OperationMode;

public abstract class BaseTSet<T> implements TSet<T> {
  private static final Logger LOG = Logger.getLogger(BaseTSet.class.getName());
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
   * Defines if the TSet is Mutable or not
   */
  private boolean isMutable = false;
  /**
   * The configuration
   */
  protected Config config;

  /**
   * Possible Types of state in a TSet
   */
  public enum StateType {
    /**
     * Local state which is updated and maintained by each parallel task
     */
    LOCAL,
    /**
     * Distributed state is when each task has only access to a subset of the whole data
     * for example if the data set has N points and T tasks each task will access N/T points
     */
    DISTRIBUTED,
    /**
     * Replicated state is state that is made available as a whole to each task
     */
    REPLICATED
  }

  /**
   * The type of the TSet
   */
  private StateType stateType = StateType.DISTRIBUTED;

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
  public TSet<T> setParallelism(int parallelism) {
    this.parallel = parallelism;
    return this;
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
  public DirectTset<T> direct() {
    DirectTset<T> direct = new DirectTset<>(config, builder, this);
    return direct;
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
  public AllReduceTSet<T> allReduce(ReduceFunction<T> reduceFn) {
    AllReduceTSet<T> reduce = new AllReduceTSet<T>(config, builder, this, reduceFn);
    children.add(reduce);
    return reduce;
  }

  @Override
  public AllGatherTSet<T> allGather() {
    AllGatherTSet<T> gather = new AllGatherTSet<>(config, builder, this);
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
  public SinkTSet<T> sink(Sink<T> sink) {
    SinkTSet<T> sinkTSet = new SinkTSet<>(config, builder, this, sink);
    children.add(sinkTSet);
    return sinkTSet;
  }

  @Override
  public ReplicateTSet<T> replicate(int replications) {
    if (parallel != 1) {
      String msg = "TSets with parallelism 1 can be replicated: " + parallel;
      LOG.log(Level.SEVERE, msg);
      throw new RuntimeException(msg);
    }

    ReplicateTSet<T> cloneTSet = new ReplicateTSet<>(config, builder, this, replications);
    children.add(cloneTSet);
    return cloneTSet;
  }

  @Override
  public TSet<T> cache() {
    // todo: why cant we add a single cache tset here?
    DirectTset<T> direct = new DirectTset<>(config, builder, this);
    children.add(direct);
    CacheTSet<T> cacheTSet = new CacheTSet<>(config, builder, direct);
    direct.children.add(cacheTSet);
    return cacheTSet;
  }

  @Override
  public void build() {
    // first build our selves
    baseBuild();

    // then build children
    for (BaseTSet<?> c : children) {
      c.build();
    }
  }

  public boolean isMutable() {
    return isMutable;
  }

  public void setMutable(boolean mutable) {
    isMutable = mutable;
  }

  public StateType getStateType() {
    return stateType;
  }

  public void setStateType(StateType stateType) {
    this.stateType = stateType;
  }

  public abstract boolean baseBuild();

  static <T> boolean isKeyedInput(BaseTSet<T> parent) {
    return parent instanceof KeyedGatherTSet || parent instanceof KeyedReduceTSet
        || parent instanceof KeyedPartitionTSet;
  }

  static <T> boolean isIterableInput(BaseTSet<T> parent, OperationMode mode) {
    if (mode == OperationMode.STREAMING) {
      if (parent instanceof ReduceTSet) {
        return false;
      } else if (parent instanceof KeyedReduceTSet) {
        return false;
      } else if (parent instanceof GatherTSet || parent instanceof KeyedGatherTSet) {
        return true;
      } else if (parent instanceof AllReduceTSet) {
        return false;
      } else if (parent instanceof AllGatherTSet) {
        return true;
      } else if (parent instanceof PartitionTSet || parent instanceof KeyedPartitionTSet) {
        return true;
      } else if (parent instanceof ReplicateTSet) {
        return false;
      } else {
        throw new RuntimeException("Failed to build un-supported operation: " + parent);
      }
    } else {
      if (parent instanceof ReduceTSet) {
        return false;
      } else if (parent instanceof KeyedReduceTSet) {
        return true;
      } else if (parent instanceof GatherTSet || parent instanceof KeyedGatherTSet) {
        return true;
      } else if (parent instanceof AllReduceTSet) {
        return false;
      } else if (parent instanceof AllGatherTSet) {
        return true;
      } else if (parent instanceof PartitionTSet || parent instanceof KeyedPartitionTSet) {
        return true;
      } else if (parent instanceof ReplicateTSet) {
        return true;
      } else {
        throw new RuntimeException("Failed to build un-supported operation: " + parent);
      }
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

  /**
   * Build the connection
   *
   * @param connection connection
   */
  abstract void buildConnection(ComputeConnection connection);

  /**
   * Override the parallelism
   *
   * @return if overide, return value, otherwise -1
   */
  protected int overrideParallelism() {
    return -1;
  }

  /**
   * Override the parallelism if operations require differently
   *
   * @return new parallelism
   */
  protected <K> int calculateParallelism(BaseTSet<K> parent) {
    int p;
    if (parent.overrideParallelism() != -1) {
      p = parent.overrideParallelism();
      LOG.log(Level.WARNING, String.format("Overriding parallelism "
          + "specified %d override value %d", parallel, p));
    } else {
      p = parallel;
    }
    return p;
  }

  protected String generateName(String prefix, BaseTSet parent) {
    if (name != null) {
      return name;
    } else {
      if (parent == null) {
        return prefix + "-" + new Random().nextInt(100);
      } else {
        return prefix + "-" + parent.getName();
      }
    }
  }
}
