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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;

/**
 * Hashing selector, that does hash based selection for keys
 */
public class HashingSelector implements DestinationSelector {
  private static final Logger LOG = Logger.getLogger(HashingSelector.class.getName());

  private Map<Integer, List<Integer>> destination = new HashMap<>();
  private MessageType keyType = null;
  private MessageType dataType = null;

  @Override
  public void prepare(Communicator comm, Set<Integer> sources, Set<Integer> destinations) {
    prepare(comm, sources, destinations, null, null);
  }

  @Override
  public void prepare(Communicator comm, Set<Integer> sources, Set<Integer> destinations,
                      MessageType kType, MessageType dType) {
    this.keyType = kType;
    this.dataType = dType;
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
    if (key != null && key.getClass().isArray()) {
      next = Math.abs(getArrayHashCode(key, keyType) % destinations.size());
    } else {
      next = Math.abs(key.hashCode()) % destinations.size();
    }
    return destinations.get(next);
  }

  private int getArrayHashCode(Object key, MessageType type) {
    if (type == MessageTypes.OBJECT || keyType == null) {
      if (key instanceof byte[]) {
        return Arrays.hashCode((byte[]) key);
      } else if (key instanceof int[]) {
        return Arrays.hashCode((int[]) key);
      } else if (key instanceof long[]) {
        return Arrays.hashCode((long[]) key);
      } else if (key instanceof double[]) {
        return Arrays.hashCode((double[]) key);
      } else if (key instanceof float[]) {
        return Arrays.hashCode((float[]) key);
      } else if (key instanceof short[]) {
        return Arrays.hashCode((short[]) key);
      } else if (key instanceof char[]) {
        return Arrays.hashCode((char[]) key);
      } else {
        throw new UnsupportedOperationException("Array type of " + key.getClass().getSimpleName()
            + " Not currently supported");
      }
    } else {
      if (type == MessageTypes.BYTE_ARRAY) {
        return Arrays.hashCode((byte[]) key);
      } else if (type == MessageTypes.INTEGER_ARRAY) {
        return Arrays.hashCode((int[]) key);
      } else if (type == MessageTypes.LONG_ARRAY) {
        return Arrays.hashCode((long[]) key);
      } else if (type == MessageTypes.DOUBLE_ARRAY) {
        return Arrays.hashCode((double[]) key);
      } else if (type == MessageTypes.FLOAT_ARRAY) {
        return Arrays.hashCode((float[]) key);
      } else if (type == MessageTypes.SHORT_ARRAY) {
        return Arrays.hashCode((short[]) key);
      } else if (type == MessageTypes.CHAR_ARRAY) {
        return Arrays.hashCode((char[]) key);
      } else {
        throw new UnsupportedOperationException("Array type of " + key.getClass().getSimpleName()
            + " Not currently supported");
      }
    }
  }

  @Override
  public int next(int source, Object data) {
    List<Integer> destinations = destination.get(source);
    int next;
    if (data != null && data.getClass().isArray()) {
      next = Math.abs(getArrayHashCode(data, dataType) % destinations.size());
    } else {
      next = data.hashCode() % destinations.size();
    }
    return destinations.get(next);
  }

  public void commit(int source, int dest) {
  }
}
