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

import java.nio.channels.SelectableChannel;

/**
 * The select handler, this will be called by the selector
 */
public interface SelectHandler {
  /**
   * Handle read availability of channel
   * @param channel socket channel
   */
  void handleRead(SelectableChannel channel);

  /**
   * Handle write availability of channel
   * @param channel socket channel
   */
  void handleWrite(SelectableChannel channel);

  /**
   * Handle accept availability of channel
   * @param channel socket channel
   */
  void handleAccept(SelectableChannel channel);

  /**
   * Handle a connection ready for channel
   * @param channel socket channel
   */
  void handleConnect(SelectableChannel channel);

  /**
   * Handle error of channel
   * @param channel socket channel
   */
  void handleError(SelectableChannel channel);
}
