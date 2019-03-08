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
import edu.iu.dsc.tws.api.tset.link.BaseTLink;
import edu.iu.dsc.tws.api.tset.ops.IterableFlatMapOp;
import edu.iu.dsc.tws.common.config.Config;

public class IFlatMapTSet<T, P> extends BaseTSet<T> {
  private BaseTLink<P> parent;

  private IterableFlatMapFunction<P, T> mapFn;

  public IFlatMapTSet(Config cfg, TSetBuilder bldr, BaseTLink<P> parent,
                      IterableFlatMapFunction<P, T> mapFunc) {
    super(cfg, bldr);
    this.parent = parent;
    this.mapFn = mapFunc;
  }

  public boolean baseBuild() {
    boolean isIterable = TSetUtils.isIterableInput(parent, builder.getOpMode());
    boolean keyed = TSetUtils.isKeyedInput(parent);

    // lets override the parallelism
    int p = calculateParallelism(parent);
    mapFn.addInputs(inputMap);
    ComputeConnection connection = builder.getTaskGraphBuilder().addCompute(generateName(
        "i-flat-map", parent), new IterableFlatMapOp<>(mapFn, isIterable, keyed), p);
    parent.buildConnection(connection);
    return true;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
  }
}

