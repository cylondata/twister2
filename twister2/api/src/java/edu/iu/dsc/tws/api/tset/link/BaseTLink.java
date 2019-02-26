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

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.tset.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.FlatMapTSet;
import edu.iu.dsc.tws.api.tset.IFlatMapTSet;
import edu.iu.dsc.tws.api.tset.IMapTSet;
import edu.iu.dsc.tws.api.tset.IterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.MapFunction;
import edu.iu.dsc.tws.api.tset.MapTSet;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.SinkTSet;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;

public abstract class BaseTLink<T> implements TLink<T> {

  /**
   * The children of this set
   */
  protected List<TBase<?>> children;

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

  public BaseTLink(Config cfg, TaskGraphBuilder bldr) {
    this.children = new ArrayList<>();
    this.builder = bldr;
    this.config = cfg;
  }

  @Override
  public TLink<T> setName(String n) {
    this.name = n;
    return null;
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
    return null;
  }

  @Override
  public <P> FlatMapTSet<P, T> flatMap(FlatMapFunction<T, P> mapFn) {
    return null;
  }

  @Override
  public <P> IMapTSet<P, T> map(IterableMapFunction<T, P> mapFn) {
    return null;
  }

  @Override
  public <P> IFlatMapTSet<P, T> flatMap(IterableFlatMapFunction<T, P> mapFn) {
    return null;
  }

  @Override
  public SinkTSet<T> sink(Sink<T> sink) {
    return null;
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
