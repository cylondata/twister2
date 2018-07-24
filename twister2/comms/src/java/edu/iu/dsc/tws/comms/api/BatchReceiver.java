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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;

/**
 * Batch receiver, with iterator for receiving
 */
public interface BatchReceiver {
  /**
   * Initialize the receiver
   * @param cfg configuration
   * @param op the operation
   * @param expectedIds expected ids
   */
  void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds);

  /**
   * Receive to specific target
   * @param target the target
   * @param it iterator with messages
   */
  void receive(int target, Iterator<Object> it);
}
