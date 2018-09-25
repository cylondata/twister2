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
import java.util.List;
import java.util.Set;

import edu.iu.dsc.tws.comms.api.DestinationSelector;
import edu.iu.dsc.tws.comms.api.MessageType;

/**
 * Returns the destination id based on the key provided. This provides a random assignment based
 * on the key that is provided, However it will produce the same value for the same key every time.
 */
public class SimpleKeyBasedSelector implements DestinationSelector {

  private List<Integer> sourceList = new ArrayList<>();
  private List<Integer> destinationList = new ArrayList<>();
  private int numDestinations = 0;
  private MessageType keyType = MessageType.INTEGER;

  @Override
  public void prepare(Set<Integer> sources, Set<Integer> destinations) {
    sourceList.addAll(sources);
    destinationList.addAll(destinations);
    numDestinations = destinationList.size();
  }

  @Override
  public void prepare(MessageType kType, Set<Integer> sources, Set<Integer> destinations) {
    this.keyType = kType;
    sourceList.addAll(sources);
    destinationList.addAll(destinations);
    numDestinations = destinationList.size();
  }

  @Override
  public int next(int source, Object data) {
    return 0;
  }

  @Override
  public int next(int source, Object key, Object data) {
    if (key instanceof Integer) {
      return getIntegerKeyBasedId((Integer) key);
    }
    return 0;
  }

  private int getIntegerKeyBasedId(Integer key) {
    int index = Math.abs(key) % numDestinations;
    return destinationList.get(index);
  }

  @Override
  public void commit(int source, int next) {

  }

}
