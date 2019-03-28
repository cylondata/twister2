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
import edu.iu.dsc.tws.api.tset.fn.KIterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.KIterableMapFunction;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunction;
import edu.iu.dsc.tws.api.tset.ops.TaskKeySelectorImpl;
import edu.iu.dsc.tws.api.tset.ops.TaskPartitionFunction;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.KIterableFlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.KIterableMapTSet;
import edu.iu.dsc.tws.api.tset.sets.SinkTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;

public class KeyedPartitionTLink<K, V> extends KeyValueTLink<K, V> {
  private BaseTSet<V> parent;

  private PartitionFunction<K> partitionFunction;

  private Selector<K, V> selector;

  public KeyedPartitionTLink(Config cfg, TSetEnv tSetEnv, BaseTSet<V> prnt,
                             PartitionFunction<K> parFn, Selector<K, V> selc) {
    super(cfg, tSetEnv);
    this.parent = prnt;
    this.partitionFunction = parFn;
    this.selector = selc;
    this.name = "keyed-partition-" + parent.getName();
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

  public <O> KIterableFlatMapTSet<K, V, O> flatMap(KIterableFlatMapFunction<K, V, O> mapFn,
                                                   int parallelism) {
    KIterableFlatMapTSet<K, V, O> set = new KIterableFlatMapTSet<>(config, tSetEnv, this,
        mapFn, parallelism);
    children.add(set);
    return set;
  }

  public SinkTSet<V> sink(Sink<V> sink, int parallelism) {
    SinkTSet<V> sinkTSet = new SinkTSet<>(config, tSetEnv, this, sink, parallelism);
    children.add(sinkTSet);
    tSetEnv.run();
    return sinkTSet;
  }

  @Override
  public boolean baseBuild() {
    return true;
  }

  public PartitionFunction<K> getPartitionFunction() {
    return partitionFunction;
  }

  public Selector<K, V> getSelector() {
    return selector;
  }

  public void buildConnection(ComputeConnection connection) {
    DataType keyType = TSetUtils.getDataType(getClassK());
    DataType dataType = TSetUtils.getDataType(getClassV());
    connection.keyedPartition(parent.getName(), Constants.DEFAULT_EDGE, keyType, dataType,
        new TaskPartitionFunction<K>(partitionFunction), new TaskKeySelectorImpl<>(selector));
  }

  @Override
  public KeyedPartitionTLink<K, V> setName(String n) {
    super.setName(n);
    return this;
  }
}
