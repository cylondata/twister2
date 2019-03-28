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
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.IterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.fn.MapFunction;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.FlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.IterableFlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.IterableMapTSet;
import edu.iu.dsc.tws.api.tset.sets.MapTSet;
import edu.iu.dsc.tws.api.tset.sets.SinkTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;

public class ReplicateTLink<T> extends edu.iu.dsc.tws.api.tset.link.BaseTLink {
  private BaseTSet<T> parent;

  private int replications;

  public ReplicateTLink(Config cfg, TSetEnv tSetEnv, BaseTSet<T> prnt, int reps) {
    super(cfg, tSetEnv);
    this.parent = prnt;
    this.name = "clone-" + parent.getName();
    this.replications = reps;
  }

  @Override
  public int overrideParallelism() {
    return replications;
  }

  public <P> MapTSet<P, T> map(MapFunction<T, P> mapFn) {
    MapTSet<P, T> set = new MapTSet<P, T>(config, tSetEnv, this, mapFn, replications);
    children.add(set);
    return set;
  }

  public <P> FlatMapTSet<P, T> flatMap(FlatMapFunction<T, P> mapFn) {
    FlatMapTSet<P, T> set = new FlatMapTSet<P, T>(config, tSetEnv, this, mapFn,
        replications);
    children.add(set);
    return set;
  }

  public <P> IterableMapTSet<P, T> map(IterableMapFunction<T, P> mapFn) {
    IterableMapTSet<P, T> set = new IterableMapTSet<>(config, tSetEnv, this,
        mapFn, replications);
    children.add(set);
    return set;
  }

  public <P> IterableFlatMapTSet<P, T> flatMap(IterableFlatMapFunction<T, P> mapFn) {
    IterableFlatMapTSet<P, T> set = new IterableFlatMapTSet<>(config, tSetEnv, this,
        mapFn, replications);
    children.add(set);
    return set;
  }

  public SinkTSet<T> sink(Sink<T> sink) {
    SinkTSet<T> sinkTSet = new SinkTSet<>(config, tSetEnv, this, sink,
        replications);
    children.add(sinkTSet);
    tSetEnv.run();
    return sinkTSet;
  }

  @Override
  public String getName() {
    return parent.getName();
  }

  @Override
  public boolean baseBuild() {
    return true;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
    DataType dataType = TSetUtils.getDataType(getType());

    connection.broadcast(parent.getName(), Constants.DEFAULT_EDGE, dataType);
  }

  @Override
  public ReplicateTLink<T> setName(String n) {
    super.setName(n);
    return this;
  }
}
