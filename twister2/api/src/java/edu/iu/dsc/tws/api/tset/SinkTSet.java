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

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.tset.ops.SinkOp;
import edu.iu.dsc.tws.common.config.Config;

public class SinkTSet<T> extends BaseTSet<T> {
  private Sink<T> sink;

  private BaseTSet<T> parent;

  public SinkTSet(Config cfg, TaskGraphBuilder bldr, BaseTSet<T> prnt, Sink<T> s) {
    super(cfg, bldr);
    this.sink = s;
    this.parent = prnt;
    this.name = "sink-" + parent.getName();
  }

  @Override
  public boolean baseBuild() {
    boolean isIterable = isIterableInput(parent);
    boolean keyed = isKeyedInput(parent);
    // lets override the parallelism
    int p = calculateParallelism(parent);

    ComputeConnection connection = builder.addSink(getName(),
        new SinkOp<T>(sink, isIterable, keyed), p);
    parent.buildConnection(connection);
    return true;
  }

  @Override
  void buildConnection(ComputeConnection connection) {

  }
}
