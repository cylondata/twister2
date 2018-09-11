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

import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;

public interface MultiMessageReceiver {
  /**
   * Initialize the message receiver with tasks from which messages are expected
   * For each sub edge in graph, for each path, gives the expected task ids
   *
   * subedge -> (path -> ids)
   *
   * @param expectedIds expected task ids
   */
  void init(Config cfg, DataFlowOperation op,
            Map<Integer, Map<Integer, List<Integer>>> expectedIds);

  /**
   * The actual message callback
   *
   * @param object the actual message
   */
  boolean onMessage(int source, int path, int target, int flags, Object object);

  boolean progress();
}
