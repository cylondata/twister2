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

package edu.iu.dsc.tws.api.tset.sets.streaming;


import edu.iu.dsc.tws.api.task.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.MapFunction;

public class StreamingMapTSet<T, P> extends StreamingBaseTSet<P> {

  private MapFunction<T, P> mapFn;

  public StreamingMapTSet(TSetEnvironment tSetEnv, MapFunction<T, P> mapFunc, int parallelism) {
    super(tSetEnv, TSetUtils.generateName("smap"), parallelism);
    this.mapFn = mapFunc;
  }

/*  @Override
  public void build(TSetGraph tSetGraph) {
//    boolean isIterable = TSetUtils.isIterableInput(parent, tSetEnv.getTSetBuilder().getOpMode());
//    boolean keyed = TSetUtils.isKeyedInput(parent);
//    int p = calculateParallelism(parent);
//    ComputeConnection connection = tSetEnv.getTSetBuilder().getTaskGraphBuilder().
//        addCompute(getName(), new MapOp<>(mapFn, isIterable, keyed), p);
//    parent.buildConnection(connection);
//    return true;
  }*/

  @Override
  protected ICompute getTask() {
    return null;
  }

  @Override
  public StreamingMapTSet<T, P> setName(String n) {
    rename(n);
    return this;
  }
}
