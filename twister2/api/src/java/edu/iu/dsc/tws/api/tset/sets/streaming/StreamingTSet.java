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

import java.util.Collection;
import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingTLink;
import edu.iu.dsc.tws.api.tset.sets.TSet;

public interface StreamingTSet<T> extends TSet<T> {
  @Override
  StreamingTSet<T> setName(String name);

  @Override
  StreamingTLink<T, T> direct();

  @Override
  StreamingTLink<T, T> reduce(ReduceFunc<T> reduceFn);

  @Override
  StreamingTLink<T, T> allReduce(ReduceFunc<T> reduceFn);

  @Override
  StreamingTLink<T, T> partition(PartitionFunc<T> partitionFn, int targetParallelism);

  @Override
  StreamingTLink<T, T> partition(PartitionFunc<T> partitionFn);

  @Override
  StreamingTLink<Iterator<Tuple<Integer, T>>, T> gather();

  @Override
  StreamingTLink<Iterator<Tuple<Integer, T>>, T> allGather();

  @Override
  <K, V> StreamingTupleTSet<K, V> mapToTuple(MapFunc<Tuple<K, V>, T> mapToTupleFn);

  @Override
  StreamingTLink<T, T> replicate(int replications);

  @Override
  StreamingTSet<T> union(TSet<T> unionTSet);

  @Override
  StreamingTSet<T> union(Collection<TSet<T>> tSets);
}
