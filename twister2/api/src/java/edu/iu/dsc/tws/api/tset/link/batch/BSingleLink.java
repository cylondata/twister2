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

package edu.iu.dsc.tws.api.tset.link.batch;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapCompute;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.ForEachCompute;
import edu.iu.dsc.tws.api.tset.fn.MapCompute;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.ops.MapToTupleOp;
import edu.iu.dsc.tws.api.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.api.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.api.tset.sets.batch.KeyedTSet;
import edu.iu.dsc.tws.api.tset.sinks.CacheSingleSink;

public abstract class BSingleLink<T> extends BBaseTLink<T, T> implements
    BatchTupleMappableLink<T> {

  BSingleLink(BatchTSetEnvironment env, String n, int sourceP) {
    super(env, n, sourceP, sourceP);
  }

  BSingleLink(BatchTSetEnvironment env, String n, int sourceP, int targetP) {
    super(env, n, sourceP, targetP);
  }

  @Override
  public <P> ComputeTSet<P, T> map(MapFunc<P, T> mapFn) {
    return compute(TSetUtils.generateName("map"), new MapCompute<>(mapFn));
  }

  @Override
  public <P> ComputeTSet<P, T> flatmap(FlatMapFunc<P, T> mapFn) {
    return compute(TSetUtils.generateName("flatmap"), new FlatMapCompute<>(mapFn));
  }

  @Override
  public void forEach(ApplyFunc<T> applyFunction) {
    ComputeTSet<Object, T> set = compute(TSetUtils.generateName("foreach"),
        new ForEachCompute<>(applyFunction));

    getTSetEnv().run(set);
  }

  @Override
  public <K, O> KeyedTSet<K, O, T> mapToTuple(MapFunc<Tuple<K, O>, T> genTupleFn) {
    KeyedTSet<K, O, T> set = new KeyedTSet<>(getTSetEnv(), new MapToTupleOp<>(genTupleFn),
        getTargetParallelism());

    addChildToGraph(set);

    return set;
  }

  @Override
  public CachedTSet<T> cache() {
    CachedTSet<T> cacheTSet = new CachedTSet<>(getTSetEnv(), new CacheSingleSink<T>(),
        getTargetParallelism());
    addChildToGraph(cacheTSet);

    DataObject<T> output = getTSetEnv().runAndGet(cacheTSet);
    cacheTSet.setData(output);

    return cacheTSet;
  }
}
