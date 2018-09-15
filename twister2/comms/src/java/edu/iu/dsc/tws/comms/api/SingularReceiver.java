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
package edu.iu.dsc.tws.comms.api;

import java.util.Set;

import edu.iu.dsc.tws.common.config.Config;

/**
 * Interface for receiving a single value. If the value is received successfully, it should
 * return true. This interface will be called multiple times with the same message
 * until it returns true.
 */
public interface SingularReceiver {
  /**
   * Initialize a receiver
   * @param cfg configuration
   * @param targets the target ids
   */
  void init(Config cfg, Set<Integer> targets);

  /**
   * Receive an object for a target id.
   * @param target target id
   * @param object object
   * @return true if the receive is accepted
   */
  boolean receive(int target, Object object);
}
