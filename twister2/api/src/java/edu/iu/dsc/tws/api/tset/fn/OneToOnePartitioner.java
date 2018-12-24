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
package edu.iu.dsc.tws.api.tset.fn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import edu.iu.dsc.tws.api.tset.PartitionFunction;

/**
 * Go from one task to a corresponding task
 */
public class OneToOnePartitioner<T> implements PartitionFunction<T> {

  private List<Integer> destinations;

  @Override
  public void prepare(Set<Integer> srcs, Set<Integer> dests) {
    if (srcs.size() != dests.size()) {
      throw new RuntimeException("Sources and destinations should be equal in size");
    }
    this.destinations = new ArrayList<>(dests);
    Collections.sort(destinations);
  }

  @Override
  public int partition(int sourceIndex, T val) {
    return destinations.get(sourceIndex);
  }

  @Override
  public void commit(int source, int partition) {
    // nothing to do
  }
}
