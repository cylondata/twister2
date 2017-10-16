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

import java.util.Set;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;

public interface DataFlowOperation {

  /**
   * Initialize the data flow communication
   *
   * @param config the network configuration
   * @param instancePlan instance plan
   */
  void init(Config config, MessageType type, int thisTask,
            TaskPlan instancePlan, Set<Integer> sources,
            Set<Integer> destinations, int stream, MessageReceiver receiver,
            MessageDeSerializer messageDeSerializer, MessageSerializer messageSerializer,
            MessageReceiver partialReceiver);

  /**
   * Use this to inject partial results in a distributed dataflow operation
   * @param message message
   */
  void injectPartialResult(Object message);

  /**
   * Do a partial operation, the receiving side should collect messages until all the messages
   * are received.
   * @param message
   */
  void sendPartial(Object message);

  /**
   * Indicate that a partial operation is finished
   */
  void finish();

  /**
   * Send a send message, this call will work asynchronously
   * @param message
   */
  boolean send(Object message);

  /**
   * Progress the pending dataflow operations
   */
  void progress();

  /**
   * Clean up the resources
   */
  void close();
}
