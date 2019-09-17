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
package edu.iu.dsc.tws.api.comms.packing;

import java.util.Queue;

import edu.iu.dsc.tws.api.comms.messaging.ChannelMessage;
import edu.iu.dsc.tws.api.config.Config;

public interface MessageSerializer {
  /**
   * Initialize the serializer, it should use the sendBuffs queue to pick a network buffer
   * @param cfg configuration
   * @param sendBuffs the send buffer qeue
   */
  void init(Config cfg, Queue<DataBuffer> sendBuffs);

  /**
   * This method will be called repeatedly until the message is fully built
   * @param message message to serialize
   * @param partialBuildObject the out message to serialize
   * @return a channel message
   */
  ChannelMessage build(Object message, Object partialBuildObject);
}
