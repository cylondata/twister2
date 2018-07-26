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
package edu.iu.dsc.tws.common.net.tcp;

import java.nio.channels.SocketChannel;

/**
 * Listen to channel events
 */
public interface ChannelHandler {
  /**
   * In case of an error this is called
   * @param channel the channel
   */
  void onError(SocketChannel channel);

  /**
   * The connect event
   * @param channel the channel
   * @param status weather an error occurred or success
   */
  void onConnect(SocketChannel channel, StatusCode status);

  /**
   * Closing of a socket
   * @param channel the tcp channel
   */
  void onClose(SocketChannel channel);

  /**
   * A message hae been received fully
   * @param channel the channel
   * @param readRequest the message details along with the buffers read
   */
  void onReceiveComplete(SocketChannel channel, TCPMessage readRequest);

  /**
   * A send is completely written to the channel
   * @param channel the channel
   * @param writeRequest the send request
   */
  void onSendComplete(SocketChannel channel, TCPMessage writeRequest);
}
