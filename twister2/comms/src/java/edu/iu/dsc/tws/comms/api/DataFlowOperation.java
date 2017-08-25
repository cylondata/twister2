//
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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.InstancePlan;

public interface DataFlowOperation {

  /**
   * Initialize the data flow communication
   *
   * @param config the network configuration
   * @param instancePlan instance plan
   */
  void init(Config config, InstancePlan instancePlan, List<Integer>  sources,
            List<Integer> destinations, int stream);

  /**
   * Do a partial broadcast, the receiving side should collect messages until all the messages
   * are received.
   * @param message
   */
  void partial(Message message);

  /**
   * Indicate that a partial broadcast is finished
   */
  void finish();

  /**
   * Send a complete message
   * @param message
   */
  void complete(Message message);
}
