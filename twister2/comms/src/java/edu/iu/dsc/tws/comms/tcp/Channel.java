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
package edu.iu.dsc.tws.comms.tcp;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;

public class Channel {
  private final Selector selector;

  public Channel(Selector selector) {
    this.selector = selector;
  }

  private void handleSelectedKeys() {
    Set<SelectionKey> selectedKeys = selector.selectedKeys();
    Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
    while (keyIterator.hasNext()) {
      SelectionKey key = keyIterator.next();
      keyIterator.remove();

      SelectHandler callback = (SelectHandler) key.attachment();

      if (!key.isValid()) {
        // This method key.channel() will continue to return the channel even after the
        // key is cancelled.
        callback.handleError(key.channel());
        continue;
      }

      if (key.isValid() && key.isWritable()) {
        callback.handleWrite(key.channel());
      }

      if (key.isValid() && key.isReadable()) {
        callback.handleRead(key.channel());
      }

      if (key.isValid() && key.isConnectable()) {
        callback.handleConnect(key.channel());
      }

      if (key.isValid() && key.isAcceptable()) {
        callback.handleAccept(key.channel());
      }
    }
  }

  public void clear() {

  }
}
