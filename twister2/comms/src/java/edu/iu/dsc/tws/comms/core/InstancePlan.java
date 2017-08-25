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
package edu.iu.dsc.tws.comms.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Information about the instances in which the communication happens.
 * This holds the mapping from the physical level communication addresses which are based on
 * the communication library to high level ids that are defined by upper layers.
 */
public class InstancePlan {
  /**
   * Map from executor to message channel ids, we assume unique channel ids across the cluster
   */
  private Map<Integer, Set<Integer>> executorToChannels = new HashMap<Integer, Set<Integer>>();

  /**
   * channel to executor mapping for easy access
   */
  private Map<Integer, Integer> invertedExecutorToChannels = new HashMap<Integer, Integer>();

  public int getExecutorForChannel(int channel) {
    return invertedExecutorToChannels.get(channel);
  }

  public Set<Integer> getChannelsOfExecutor(int executor) {
    return executorToChannels.get(executor);
  }
}
