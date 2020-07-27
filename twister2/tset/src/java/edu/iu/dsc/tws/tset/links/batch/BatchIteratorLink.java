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


package edu.iu.dsc.tws.tset.links.batch;

import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.fn.FlatMapIterCompute;
import edu.iu.dsc.tws.tset.fn.ForEachIterCompute;
import edu.iu.dsc.tws.tset.fn.MapIterCompute;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedTSet;

public abstract class BatchIteratorLink<T> extends BatchTLinkImpl<Iterator<T>, T> {

  BatchIteratorLink(BatchEnvironment env, String n, int sourceP, Schema schema) {
    this(env, n, sourceP, sourceP, schema);
  }

  BatchIteratorLink(BatchEnvironment env, String n, int sourceP, int targetP,
                    Schema schema) {
    super(env, n, sourceP, targetP, schema);
  }

  protected BatchIteratorLink() {
  }

  @Override
  public <P> ComputeTSet<P> map(MapFunc<T, P> mapFn) {
    return compute("map", new MapIterCompute<>(mapFn));
  }

  @Override
  public <P> ComputeTSet<P> flatmap(FlatMapFunc<T, P> mapFn) {
    return compute("flatmap", new FlatMapIterCompute<>(mapFn));
  }

  @Override
  public <K, V> KeyedTSet<K, V> mapToTuple(MapFunc<T, Tuple<K, V>> mapToTupFn) {
    KeyedTSet<K, V> set = new KeyedTSet<>(getTSetEnv(), new MapIterCompute<>(mapToTupFn),
        getTargetParallelism(), getSchema());

    addChildToGraph(set);

    return set;
  }

  @Override
  public ComputeTSet<Object> lazyForEach(ApplyFunc<T> applyFunction) {
    return compute("foreach", new ForEachIterCompute<>(applyFunction));
  }

  @Override
  public void forEach(ApplyFunc<T> applyFunction) {
    ComputeTSet<Object> set = lazyForEach(applyFunction);

    getTSetEnv().run(set);
  }
}
