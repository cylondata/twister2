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
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.fn.GatherFlatMapCompute;
import edu.iu.dsc.tws.tset.fn.GatherForEachCompute;
import edu.iu.dsc.tws.tset.fn.GatherMapCompute;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sinks.GatherCacheSink;

/**
 * This is the Tlinks used by gather operations. Specific operations such as map, flatmap, cache,
 * etc will be done on the tuple value only (key will be dropped, as key is an information
 * forcibly attached at the communication level). If the key information is required, users can
 * use the compute methods which enables the use of Iterator<Tuple<Integer, T>>
 *
 * @param <T> value type
 */
public abstract class BBaseGatherLink<T> extends BBaseTLink<Iterator<Tuple<Integer, T>>, T> {

  BBaseGatherLink(BatchTSetEnvironment env, String n, int sourceP) {
    this(env, n, sourceP, sourceP);
  }

  BBaseGatherLink(BatchTSetEnvironment env, String n, int sourceP, int targetP) {
    super(env, n, sourceP, targetP);
  }
/*  public <P> ComputeTSet<P, Iterator<T>>
computeWithoutKey(Compute<P, Iterator<T>> computeFunction) {
    computeFnWrapper = new ComputeCollectorWrapper<P, Integer, T>(computeFunction);
    return null;
  }

  public <P> ComputeTSet<P, Iterator<T>>
  computeWithoutKey(ComputeCollector<P, Iterator<T>> computeFunction) {
    ComputeCollectorWrapper<P, Integer, T> computeFnWrapper =
        new ComputeCollectorWrapper<>(computeFunction);
    return compute("computec");
  }*/

  @Override
  public <O> ComputeTSet<O, Iterator<Tuple<Integer, T>>> map(MapFunc<O, T> mapFn) {
    GatherMapCompute<O, T> comp = new GatherMapCompute<>(mapFn);
    return compute("map", comp);
  }

  @Override
  public <O> ComputeTSet<O, Iterator<Tuple<Integer, T>>> flatmap(FlatMapFunc<O, T> mapFn) {
    GatherFlatMapCompute<O, T> comp = new GatherFlatMapCompute<>(mapFn);
    return compute("map", comp);
  }

  @Override
  public void forEach(ApplyFunc<T> applyFunction) {
    ComputeTSet<Object, Iterator<Tuple<Integer, T>>> set = lazyForEach(applyFunction);
//    addChildToGraph(foreach);
    getTSetEnv().run(set);
  }

  @Override
  public ComputeTSet<Object, Iterator<Tuple<Integer, T>>> lazyForEach(ApplyFunc<T> applyFunction) {
    GatherForEachCompute<T> comp = new GatherForEachCompute<>(applyFunction);
    return compute("foreach", comp);
  }

  @Override
  public CachedTSet<T> cache() {
    CachedTSet<T> cacheTSet = new CachedTSet<>(getTSetEnv(), new GatherCacheSink<T>(),
        getTargetParallelism());
    addChildToGraph(cacheTSet);

    DataObject<T> output = getTSetEnv().runAndGet(cacheTSet);
    cacheTSet.setData(output);

    return cacheTSet;
  }
}
