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

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.Constants;
import edu.iu.dsc.tws.api.tset.Selector;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.KIterableMapFunction;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunction;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunction;
import edu.iu.dsc.tws.api.tset.ops.ReduceOpFunction;
import edu.iu.dsc.tws.api.tset.ops.TaskKeySelectorImpl;
import edu.iu.dsc.tws.api.tset.ops.TaskPartitionFunction;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.KIterableMapTSet;
import edu.iu.dsc.tws.api.tset.sets.SinkTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;

public class KeyedReduceTLink<K, V> extends KeyValueTLink<K, V> {
  private ReduceFunction<V> reduceFn;

  private BaseTSet<V> parent;

  private PartitionFunction<K> partitionFunction;

  private Selector<K, V> selector;

  public KeyedReduceTLink(Config cfg, TSetEnv tSetEnv, BaseTSet<V> prnt,
                          ReduceFunction<V> rFn, PartitionFunction<K> parFn,
                          Selector<K, V> selec) {
    super(cfg, tSetEnv);
    this.parent = prnt;
    this.reduceFn = rFn;
    this.partitionFunction = parFn;
    this.selector = selec;
    this.name = "keyed-reduce-" + parent.getName();
  }

  @Override
  public String getName() {
    return parent.getName();
  }

  public <O> KIterableMapTSet<K, V, O> map(KIterableMapFunction<K, V, O> mapFn, int parallelism) {
    KIterableMapTSet<K, V, O> set = new KIterableMapTSet<>(config, tSetEnv, this, mapFn,
        parallelism);
    children.add(set);
    return set;
  }

//  public <P> IterableFlatMapTSet<P, T> flatMap(IterableFlatMapFunction<T, P> mapFn,
//                                               int parallelism) {
//    IterableFlatMapTSet<P, T> set = new IterableFlatMapTSet<>(config, tSetEnv, this,
//        mapFn, parallelism);
//    children.add(set);
//    return set;
//  }

  public SinkTSet<V> sink(Sink<V> sink, int parallelism) {
    SinkTSet<V> sinkTSet = new SinkTSet<>(config, tSetEnv, this, sink, parallelism);
    children.add(sinkTSet);
    tSetEnv.run();
    return sinkTSet;
  }

  @Override
  public boolean baseBuild() {
    return false;
  }

  public ReduceFunction<V> getReduceFn() {
    return reduceFn;
  }

  public PartitionFunction<K> getPartitionFunction() {
    return partitionFunction;
  }

  public void buildConnection(ComputeConnection connection) {
    DataType keyType = TSetUtils.getDataType(getClassK());
    DataType dataType = TSetUtils.getDataType(getClassV());
    connection.keyedReduce(parent.getName(), Constants.DEFAULT_EDGE,
        new ReduceOpFunction<>(reduceFn), keyType, dataType,
        new TaskPartitionFunction<K>(partitionFunction), new TaskKeySelectorImpl<>(selector));
  }

  @Override
  public KeyedReduceTLink<K, V> setName(String n) {
    super.setName(n);
    return this;
  }
}
