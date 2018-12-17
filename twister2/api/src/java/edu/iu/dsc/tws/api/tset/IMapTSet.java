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
import edu.iu.dsc.tws.api.tset.ops.IterableMapOp;
import edu.iu.dsc.tws.common.config.Config;

public class IMapTSet<T, P> extends BaseTSet<T> {
  private BaseTSet<P> parent;

  private IterableMapFunction<P, T> mapFn;

  public IMapTSet(Config cfg, TaskGraphBuilder builder,
                  BaseTSet<P> parent, IterableMapFunction<P, T> mapFunc) {
    super(cfg, builder);
    this.parent = parent;
    this.mapFn = mapFunc;
  }

  @SuppressWarnings("unchecked")
  public boolean baseBuild() {
    boolean isIterable = isIterableInput(parent);

    int p = calculateParallelism(parent);

    ComputeConnection connection = builder.addCompute(generateName("i-map", parent),
        new IterableMapOp<>(mapFn, isIterable), p);
    parent.buildConnection(connection);
    return true;
  }

  @Override
  void buildConnection(ComputeConnection connection) {
  }
}
