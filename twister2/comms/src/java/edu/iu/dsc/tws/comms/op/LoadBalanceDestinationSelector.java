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
package edu.iu.dsc.tws.comms.op;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.comms.api.MessageType;

public class LoadBalanceDestinationSelector implements DestinationSelector {
  private Map<Integer, List<Integer>> destination = new HashMap<>();

  private Map<Integer, Integer> destinationIndexes = new HashMap<>();

  @Override
  public void prepare(Set<Integer> sources, Set<Integer> destinations) {
    for (int s : sources) {
      destination.put(s, new ArrayList<>(destinations));
      destinationIndexes.put(s, 0);
    }
  }

  @Override
  public void prepare(MessageType type, Set<Integer> sources, Set<Integer> destinations) {
  }

  @Override
  public int next(int source) {
    return 0;
  }

  @Override
  public int next(int source, Object key) {
    return 0;
  }
}
