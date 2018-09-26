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
package edu.iu.dsc.tws.comms.op.selectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.DestinationSelector;
import edu.iu.dsc.tws.comms.api.MessageType;

/**
 * Hashing selector, that does hash based selection for keys
 */
public class HashingSelector implements DestinationSelector {
  private static final Logger LOG = Logger.getLogger(HashingSelector.class.getName());

  private Map<Integer, List<Integer>> destination = new HashMap<>();

  @Override
  public void prepare(Set<Integer> sources, Set<Integer> destinations) {
    initialize(sources, destinations);
  }

  private void initialize(Set<Integer> sources, Set<Integer> destinations) {
    for (int s : sources) {
      ArrayList<Integer> destList = new ArrayList<>(destinations);
      destination.put(s, destList);
    }
  }

  @Override
  public void prepare(MessageType type, Set<Integer> sources, Set<Integer> destinations) {
    initialize(sources, destinations);
  }

  @Override
  public int next(int source, Object key, Object data) {
    List<Integer> destinations = destination.get(source);
    int next = key.hashCode() % destinations.size();
    return destinations.get(next);
  }

  @Override
  public int next(int source, Object data) {
    List<Integer> destinations = destination.get(source);
    int next = data.hashCode() % destinations.size();
    return destinations.get(next);
  }

  public void commit(int source, int dest) {
  }
}
