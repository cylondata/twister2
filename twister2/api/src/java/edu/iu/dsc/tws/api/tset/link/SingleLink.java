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

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.Apply;
import edu.iu.dsc.tws.api.tset.fn.FlatMapCompute;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.ForEachCompute;
import edu.iu.dsc.tws.api.tset.fn.MapCompute;
import edu.iu.dsc.tws.api.tset.fn.MapFunction;
import edu.iu.dsc.tws.api.tset.ops.MapToTupleOp;
import edu.iu.dsc.tws.api.tset.sets.ComputeTSet;
import edu.iu.dsc.tws.api.tset.sets.KeyedTSet;

public abstract class SingleLink<T> extends BaseTLink<T, T> implements
    TupleMappableLink<T> {

  protected SingleLink(TSetEnvironment env, String n, int sourceP) {
    super(env, n, sourceP, sourceP);
  }

  protected SingleLink(TSetEnvironment env, String n, int sourceP, int targetP) {
    super(env, n, sourceP, targetP);
  }

  @Override
  public <P> ComputeTSet<P, T> map(MapFunction<P, T> mapFn) {
    return compute(TSetUtils.generateName("map"), new MapCompute<>(mapFn));
  }

  @Override
  public <P> ComputeTSet<P, T> flatmap(FlatMapFunction<P, T> mapFn) {
    return compute(TSetUtils.generateName("flatmap"), new FlatMapCompute<>(mapFn));
  }

  @Override
  public void forEach(Apply<T> applyFunction) {
    ComputeTSet<Object, T> set = compute(TSetUtils.generateName("foreach"),
        new ForEachCompute<>(applyFunction));

    getTSetEnv().run(set);
  }

  @Override
  public <K, O> KeyedTSet<K, O, T> mapToTuple(MapFunction<Tuple<K, O>, T> genTupleFn) {
    KeyedTSet<K, O, T> set = new KeyedTSet<>(getTSetEnv(), new MapToTupleOp<>(genTupleFn),
        getTargetParallelism());

    addChildToGraph(set);

    return set;
  }
}
