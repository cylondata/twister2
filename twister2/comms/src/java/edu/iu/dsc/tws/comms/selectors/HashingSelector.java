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
package edu.iu.dsc.tws.comms.selectors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;

/**
 * Hashing selector, that does hash based selection for keys
 */
public class HashingSelector implements DestinationSelector {
  private static final Logger LOG = Logger.getLogger(HashingSelector.class.getName());

  private Map<Integer, List<Integer>> destination = new HashMap<>();

  @Override
  public void prepare(Communicator comm, Set<Integer> sources, Set<Integer> destinations) {
    initialize(sources, destinations);
  }

  private void initialize(Set<Integer> sources, Set<Integer> destinations) {
    for (int s : sources) {
      ArrayList<Integer> destList = new ArrayList<>(destinations);
      destList.sort((o1, o2) -> o1 - o2);
      destination.put(s, destList);
    }
  }

  @Override
  public int next(int source, Object key, Object data) {
    List<Integer> destinations = destination.get(source);
    int next;
    if (key instanceof byte[]) {
      next = Math.abs(ByteBuffer.wrap((byte[]) key).hashCode() % destinations.size());
    } else {
      next = Math.abs(key.hashCode()) % destinations.size();
    }
    return destinations.get(next);
  }

  @Override
  public int next(int source, Object data) {
    List<Integer> destinations = destination.get(source);
    int next;
    if (data instanceof byte[]) {
      next = ByteBuffer.wrap((byte[]) data).hashCode() % destinations.size();
    } else {
      next = data.hashCode() % destinations.size();
    }
    return destinations.get(next);
  }

  public void commit(int source, int dest) {
  }
}
