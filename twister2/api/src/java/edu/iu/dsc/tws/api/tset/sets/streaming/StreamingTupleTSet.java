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

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingTLink;
import edu.iu.dsc.tws.api.tset.sets.BuildableTSet;

public interface StreamingTupleTSet<K, V, T> extends BuildableTSet {
  /**
   * Partition by key
   *
   * @param partitionFn partition function
   * @return this set
   */
  StreamingTLink<Tuple<K, T>, Tuple<K, T>> keyedPartition(PartitionFunc<K> partitionFn);
}
