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

import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.Apply;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.FlatMapIterCompute;
import edu.iu.dsc.tws.api.tset.fn.ForEachIterCompute;
import edu.iu.dsc.tws.api.tset.fn.MapFunction;
import edu.iu.dsc.tws.api.tset.fn.MapIterCompute;
import edu.iu.dsc.tws.api.tset.ops.MapToTupleIterOp;
import edu.iu.dsc.tws.api.tset.sets.ComputeTSet;
import edu.iu.dsc.tws.api.tset.sets.KeyedTSet;

public abstract class IteratorLink<T> extends BaseTLink<Iterator<T>, T>
    implements TupleMappableLink<T> {

  IteratorLink(TSetEnvironment env, String n, int sourceP) {
    this(env, n, sourceP, sourceP);
  }

  IteratorLink(TSetEnvironment env, String n, int sourceP, int targetP) {
    super(env, n, sourceP, targetP);
  }

  @Override
  public <P> ComputeTSet<P, Iterator<T>> map(MapFunction<P, T> mapFn) {
    return compute(TSetUtils.generateName("map"), new MapIterCompute<>(mapFn));
  }

  @Override
  public <P> ComputeTSet<P, Iterator<T>> flatmap(FlatMapFunction<P, T> mapFn) {
    return compute(TSetUtils.generateName("flatmap"), new FlatMapIterCompute<>(mapFn));
  }

  @Override
  public void forEach(Apply<T> applyFunction) {
    ComputeTSet<Object, Iterator<T>> set = compute(TSetUtils.generateName("foreach"),
        new ForEachIterCompute<>(applyFunction)
    );

    getTSetEnv().run(set);
  }

  @Override
  public <K, V> KeyedTSet<K, V, T> mapToTuple(MapFunction<Tuple<K, V>, T> mapToTupFn) {
    KeyedTSet<K, V, T> set = new KeyedTSet<>(getTSetEnv(), new MapToTupleIterOp<>(mapToTupFn),
        getTargetParallelism());

    addChildToGraph(set);

    return set;
  }
}
