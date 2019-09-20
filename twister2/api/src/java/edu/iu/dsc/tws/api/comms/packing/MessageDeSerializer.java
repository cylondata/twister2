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

import edu.iu.dsc.tws.api.comms.messaging.MessageHeader;
import edu.iu.dsc.tws.api.config.Config;

/**
 * Message un-packing interface.
 */
public interface MessageDeSerializer {
  /**
   * Initialize the deserializer
   * @param cfg configuration
   */
  void init(Config cfg);

  /**
   * Serilize a message
   * @param partialObject the outmessage
   * @param edge the edge
   */
  void build(Object partialObject, int edge);

  /**
   * Read the header from the buffer
   * @param buffer the buffer to read
   * @param edge edge
   * @return a message header
   */
  MessageHeader buildHeader(DataBuffer buffer, int edge);

  /**
   * Returns the data buffers for the given message.
   * @param partialObject object that contains the buffers
   * @param edge id of the edge
   * @return if single message and not keyed returns the data. if keyed returns a pair of
   * {key,data}. if there are multiple sub messages returns a list of data object for non keyed and
   * a list of {key,data} for keyed operations
   */
  Object getDataBuffers(Object partialObject, int edge);
}
