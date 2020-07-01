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
package edu.iu.dsc.tws.executor.comms;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.compute.TaskPartitioner;

public class DefaultDestinationSelector implements DestinationSelector {

  private TaskPartitioner partitioner;

  private Map<Integer, Integer> srcGlobalToIndex;
  private Map<Integer, Integer> tgtsGlobaToIndex;
  private Set<Integer> srcIndexes = new HashSet<>();
  private Set<Integer> tgtIndexes = new HashSet<>();
  private Map<Integer, Integer> indexToGlobal = new HashMap<>();

  public DefaultDestinationSelector(TaskPartitioner partitioner,
                                    Map<Integer, Integer> srcGlobalToIndex,
                                    Map<Integer, Integer> tgtsGlobalToIndex) {
    this.partitioner = partitioner;
    this.srcGlobalToIndex = srcGlobalToIndex;
    this.tgtsGlobaToIndex = tgtsGlobalToIndex;

    srcIndexes.addAll(srcGlobalToIndex.values());
    tgtIndexes.addAll(tgtsGlobalToIndex.values());
    for (Map.Entry<Integer, Integer> e : tgtsGlobalToIndex.entrySet()) {
      indexToGlobal.put(e.getValue(), e.getKey());
    }
  }

  @Override
  public void prepare(Communicator comm, Set<Integer> sources, Set<Integer> destinations) {
    if (!sources.equals(new HashSet<>(srcGlobalToIndex.keySet()))) {
      throw new RuntimeException("Sources should be equal");
    }
    if (!destinations.equals(new HashSet<>(tgtsGlobaToIndex.keySet()))) {
      throw new RuntimeException("Targets should be equal");
    }

    partitioner.prepare(srcIndexes, tgtIndexes);
  }

  @Override
  public int next(int source, Object data) {
    int partition = partitioner.partition(srcGlobalToIndex.get(source), data);
    return indexToGlobal.get(partition);
  }

  @Override
  public void commit(int source, int next) {
    partitioner.commit(srcGlobalToIndex.get(source), next);
  }

  @Override
  public int next(int source, Object key, Object data) {
    int partition = partitioner.partition(srcGlobalToIndex.get(source), key);
    return indexToGlobal.get(partition);
  }
}
