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
package edu.iu.dsc.tws.comms.dfw;

import edu.iu.dsc.tws.comms.api.MessageHeader;

public interface ChannelReceiver {
  /**
   * Receive a fully built message
   * @param header the header
   * @param object the built message
   * @return true if accepted
   */
  boolean receiveMessage(MessageHeader header, Object object);
  /**
   * For partial receives the path and
   * @param source the source
   * @param target target to be sent
   * @param path the path to be used
   * @param flags flags
   * @param message message
   * @return true if success
   */
  boolean receiveSendInternally(int source, int target, int path, int flags, Object message);

  /**
   * Handle a partially received buffers
   * @param currentMessage the message
   * @return true if accepted
   */
  default boolean handleReceivedChannelMessage(ChannelMessage currentMessage) {
    return true;
  }

  /**
   * The send has been completed
   *
   * @param message the out message
   */
  default void sendCompleted(OutMessage message) {
  }
}
