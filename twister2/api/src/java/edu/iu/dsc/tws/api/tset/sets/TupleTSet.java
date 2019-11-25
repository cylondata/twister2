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
package edu.iu.dsc.tws.api.tset.sets;

import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.link.TLink;

/**
 * Twister data set for keyed data. This would abstract the Task level keyed computations in a
 * more user friendly API.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface TupleTSet<K, V> extends TBase {
  /**
   * Set the name of the set
   *
   * @param name name
   * @return this set
   */
  TupleTSet<K, V> setName(String name);

  /**
   * Do a partition
   *
   * @param partitionFn function to choose the partition
   * @return partition link
   */
  TLink<?, ?> keyedPartition(PartitionFunc<K> partitionFn);

  /**
   * Direct communication for keyed TSets
   *
   * @return partition link
   */
  TLink<?, ?> keyedDirect();
}
