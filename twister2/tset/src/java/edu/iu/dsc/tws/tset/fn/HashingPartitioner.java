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

package edu.iu.dsc.tws.tset.fn;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;

public class HashingPartitioner<T> implements PartitionFunc<T> {
  private static final Logger LOG = Logger.getLogger(HashingPartitioner.class.getName());

  private List<Integer> destinations = new ArrayList<>();

  @Override
  public void prepare(Set<Integer> sources, Set<Integer> dests) {
    initialize(sources, dests);
  }

  @Override
  public int partition(int sourceIndex, T val) {
    int next = (int) (Math.abs((long) val.hashCode()) % destinations.size());
    return destinations.get(next);
  }

  @Override
  public void commit(int source, int partition) {

  }

  private void initialize(Set<Integer> sources, Set<Integer> dests) {
    destinations = new ArrayList<>(dests);
    destinations.sort(Comparator.comparingInt(o -> o));
  }
}
