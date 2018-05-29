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

/**
 * Interface that defines the MessageBuffer, which is used to buffer messages before sending them
 * over the network. This helps reduce network delays since the network is not filled with a large
 * amount of small messages
 */
public interface MessageBuffer {

  /**
   * Send a send message, this call will work asynchronously
   * @param source
   * @param message
   */
  boolean send(int source, Object message, int flags);

  /**
   * Send the message on a specific path
   * @param source
   * @param message
   * @param dest
   * @return
   */
  boolean send(int source, Object message, int flags, int dest);

  /**
   * Buffer messages according to the buffering logic defined before sending messages
   * @param source
   * @param message
   * @param flags
   * @return
   */
  boolean buffer(int source, Object message, int flags);

  /**
   * Buffer messages according to the buffering logic defined before sending messages
   * @param source
   * @param message
   * @param flags
   * @return
   */
  boolean buffer(int source, Object message, int flags, int dest);

  /**
   * Flush all buffered messages
   * @return
   */
  boolean flush();

  /**
   * Flush all buffered messages for this destination
   * @param dest target destination id
   * @return
   */
  boolean flush(int dest);
}
