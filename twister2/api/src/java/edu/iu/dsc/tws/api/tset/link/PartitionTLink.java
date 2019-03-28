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
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.IterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunction;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.IterableFlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.IterableMapTSet;
import edu.iu.dsc.tws.api.tset.sets.SinkTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;

public class PartitionTLink<T> extends edu.iu.dsc.tws.api.tset.link.BaseTLink<T> {
  private BaseTSet<T> parent;

  private PartitionFunction<T> partitionFunction;

  public PartitionTLink(Config cfg, TSetEnv tSetEnv, BaseTSet<T> prnt,
                        PartitionFunction<T> parFn) {
    super(cfg, tSetEnv);
    this.parent = prnt;
    this.partitionFunction = parFn;
    this.name = "partition-" + parent.getName();
  }

  @Override
  public String getName() {
    return parent.getName();
  }

  public <P> IterableMapTSet<T, P> map(IterableMapFunction<T, P> mapFn, int parallelism) {
    IterableMapTSet<T, P> set = new IterableMapTSet<>(config, tSetEnv, this, mapFn,
        parallelism);
    children.add(set);
    return set;
  }

  public <P> IterableFlatMapTSet<T, P> flatMap(IterableFlatMapFunction<T, P> mapFn,
                                               int parallelism) {
    IterableFlatMapTSet<T, P> set = new IterableFlatMapTSet<>(config, tSetEnv, this,
        mapFn, parallelism);
    children.add(set);
    return set;
  }

  public SinkTSet<T> sink(Sink<T> sink, int parallelism) {
    SinkTSet<T> sinkTSet = new SinkTSet<>(config, tSetEnv, this, sink, parallelism);
    children.add(sinkTSet);
    tSetEnv.run();
    return sinkTSet;
  }

  @Override
  public boolean baseBuild() {
    return true;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
    DataType dataType = TSetUtils.getDataType(getType());

    connection.partition(parent.getName(), Constants.DEFAULT_EDGE, dataType);
  }

  public PartitionFunction<T> getPartitionFunction() {
    return partitionFunction;
  }

  @Override
  public PartitionTLink<T> setName(String n) {
    super.setName(n);
    return this;
  }
}
