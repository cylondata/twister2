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

package edu.iu.dsc.tws.api.tset.sets;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.link.BaseTLink;
import edu.iu.dsc.tws.api.tset.ops.SinkOp;
import edu.iu.dsc.tws.common.config.Config;

public class SinkTSet<T> extends BaseTSet<T> {
  private Sink<T> sink;

  private BaseTLink<T> parent;

  public SinkTSet(Config cfg, TSetEnv tSetEnv, BaseTLink<T> prnt, Sink<T> s) {
    super(cfg, tSetEnv);
    this.sink = s;
    this.parent = prnt;
    this.name = "sink-" + parent.getName();
  }

  @Override
  public boolean baseBuild() {
    boolean isIterable = TSetUtils.isIterableInput(parent, tSetEnv.getTSetBuilder().getOpMode());
    boolean keyed = TSetUtils.isKeyedInput(parent);
    // lets override the parallelism
    int p = calculateParallelism(parent);
    sink.addInputs(inputMap);
    ComputeConnection connection = tSetEnv.getTSetBuilder().getTaskGraphBuilder().addSink(getName(),
        new SinkOp<>(sink, isIterable, keyed), p);
    parent.buildConnection(connection);
    return true;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
    throw new IllegalStateException("Build connections should not be called on a TSet");
  }

  @Override
  public SinkTSet<T> setParallelism(int parallelism) {
    this.parallel = parallelism;
    return this;
  }

  @Override
  public SinkTSet<T> setName(String n) {
    this.name = n;
    return this;
  }
}
