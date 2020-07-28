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

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.tset.env.StreamingEnvironment;
import edu.iu.dsc.tws.tset.fn.FlatMapCompute;
import edu.iu.dsc.tws.tset.fn.ForEachCompute;
import edu.iu.dsc.tws.tset.fn.MapCompute;
import edu.iu.dsc.tws.tset.sets.streaming.SComputeTSet;
import edu.iu.dsc.tws.tset.sets.streaming.SKeyedTSet;

public abstract class StreamingSingleLink<T> extends StreamingTLinkImpl<T, T> {

  StreamingSingleLink(StreamingEnvironment env, String n, int sourceP, Schema schema) {
    super(env, n, sourceP, sourceP, schema);
  }

  StreamingSingleLink(StreamingEnvironment env, String n, int sourceP, int targetP,
                      Schema schema) {
    super(env, n, sourceP, targetP, schema);
  }

  @Override
  public <P> SComputeTSet<P> map(MapFunc<T, P> mapFn) {
    return compute("smap", new MapCompute<>(mapFn));
  }

  @Override
  public <P> SComputeTSet<P> flatmap(FlatMapFunc<T, P> mapFn) {
    return compute("sflatmap", new FlatMapCompute<>(mapFn));
  }

  @Override
  public void forEach(ApplyFunc<T> applyFunction) {
    SComputeTSet<Object> set = compute("sforeach", new ForEachCompute<>(applyFunction));
  }

  @Override
  public <K, O> SKeyedTSet<K, O> mapToTuple(MapFunc<T, Tuple<K, O>> genTupleFn) {
    return this.computeToTuple("smap2tup", new MapCompute<>(genTupleFn));
  }
}
