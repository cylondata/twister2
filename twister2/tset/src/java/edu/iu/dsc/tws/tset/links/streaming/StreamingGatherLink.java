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


package edu.iu.dsc.tws.tset.links.streaming;

import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.tset.fn.GatherFlatMapCompute;
import edu.iu.dsc.tws.tset.fn.GatherForEachCompute;
import edu.iu.dsc.tws.tset.fn.GatherMapCompute;
import edu.iu.dsc.tws.tset.sets.streaming.SComputeTSet;
import edu.iu.dsc.tws.tset.sets.streaming.SKeyedTSet;

/**
 * This is the Tlinks used by gather operations. Specific operations such as map, flatmap, cache,
 * etc will be done on the tuple value only (key will be dropped, as key is an information
 * forcibly attached at the communication level). If the key information is required, users can
 * use the compute methods which enables the use of {@literal Iterator<Tuple<K, T>>}
 *
 * @param <T> value type
 */
public abstract class StreamingGatherLink<T>
    extends StreamingTLinkImpl<Iterator<Tuple<Integer, T>>, T> {

  StreamingGatherLink(StreamingTSetEnvironment env, String n, int sourceP, Schema schema) {
    this(env, n, sourceP, sourceP, schema);
  }

  StreamingGatherLink(StreamingTSetEnvironment env, String n, int sourceP, int targetP,
                      Schema schema) {
    super(env, n, sourceP, targetP, schema);
  }
/*  public <P> StreamingComputeTSet<P, Iterator<T>>
computeWithoutKey(Compute<P, Iterator<T>> computeFunction) {
    computeFnWrapper = new ComputeCollectorWrapper<P, K, T>(computeFunction);
    return null;
  }

  public <P> StreamingComputeTSet<P, Iterator<T>>
  computeWithoutKey(ComputeCollector<P, Iterator<T>> computeFunction) {
    ComputeCollectorWrapper<P, K, T> computeFnWrapper =
        new ComputeCollectorWrapper<>(computeFunction);
    return compute(("computec"));
  }*/

  @Override
  public <O> SComputeTSet<O, Iterator<Tuple<Integer, T>>> map(MapFunc<O, T> mapFn) {
    GatherMapCompute<O, T> comp = new GatherMapCompute<>(mapFn);
    return compute("smap", comp);
  }

  @Override
  public <O> SComputeTSet<O, Iterator<Tuple<Integer, T>>> flatmap(FlatMapFunc<O, T> mapFn) {
    GatherFlatMapCompute<O, T> comp = new GatherFlatMapCompute<>(mapFn);
    return compute("smap", comp);
  }

  @Override
  public <K, V> SKeyedTSet<K, V> mapToTuple(MapFunc<Tuple<K, V>, T> genTupleFn) {
    SKeyedTSet<K, V> set = new SKeyedTSet<>(getTSetEnv(), new GatherMapCompute<>(genTupleFn),
        getTargetParallelism(), getSchema());
    addChildToGraph(set);
    return set;
  }

  @Override
  public void forEach(ApplyFunc<T> applyFunction) {
    GatherForEachCompute<T> comp = new GatherForEachCompute<>(applyFunction);
    SComputeTSet<Object, Iterator<Tuple<Integer, T>>> foreach =
        compute("sforeach", comp);
    addChildToGraph(foreach);
  }
}
