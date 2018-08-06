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

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;

/**
 * The communicationProgress loop
 */
public class Progress {
  private static final Logger LOG = Logger.getLogger(Progress.class.getName());

  private Selector selector;

  public Progress() {
    try {
      selector = Selector.open();
    } catch (IOException e) {
      throw new RuntimeException("Failed to open selector");
    }
  }

  public void loop() {
    try {
      selector.selectNow();

      handleSelectedKeys();
    } catch (IOException e) {
      throw new RuntimeException("Failed to select");
    }
  }

  public void loopBlocking() {
    try {
      selector.select();

      handleSelectedKeys();
    } catch (IOException e) {
      throw new RuntimeException("Failed to select");
    }
  }

  public void loopBlocking(long blockingDuration) {
    try {
      selector.select(blockingDuration);

      handleSelectedKeys();
    } catch (IOException e) {
      throw new RuntimeException("Failed to select");
    }
  }

  public void wakeup() {
    try {
      selector.wakeup();

    } catch (Exception e) {
      LOG.warning("Failed to wakeup the selector.");
    }
  }

  private void handleSelectedKeys() {
    Set<SelectionKey> selectedKeys = selector.selectedKeys();

    Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
    while (keyIterator.hasNext()) {
      SelectionKey key = keyIterator.next();
      keyIterator.remove();

      SelectHandler callback = (SelectHandler) key.attachment();

      if (!key.isValid()) {
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

  public void registerRead(SelectableChannel channel, SelectHandler callback)
      throws ClosedChannelException {
    assert channel.keyFor(selector) == null
        || (channel.keyFor(selector).interestOps() & SelectionKey.OP_CONNECT) == 0;
    addInterest(channel, SelectionKey.OP_READ, callback);
  }

  public void unregisterRead(SelectableChannel channel) {
    removeInterest(channel, SelectionKey.OP_READ);
  }

  public boolean isReadRegistered(SelectableChannel channel) {
    return isInterestRegistered(channel, SelectionKey.OP_READ);
  }

  public void registerConnect(SelectableChannel channel, SelectHandler callback)
      throws ClosedChannelException {
    // This channel should be first use
    assert channel.keyFor(selector) == null;
    addInterest(channel, SelectionKey.OP_CONNECT, callback);
  }

  public void unregisterConnect(SelectableChannel channel) {
    removeInterest(channel, SelectionKey.OP_CONNECT);
  }

  public boolean isConnectRegistered(SelectableChannel channel) {
    return isInterestRegistered(channel, SelectionKey.OP_CONNECT);
  }

  public void registerAccept(SelectableChannel channel, SelectHandler callback)
      throws ClosedChannelException {
    addInterest(channel, SelectionKey.OP_ACCEPT, callback);
  }

  public void unregisterAccept(SelectableChannel channel) {
    removeInterest(channel, SelectionKey.OP_ACCEPT);
  }

  public boolean isAcceptRegistered(SelectableChannel channel) {
    return isInterestRegistered(channel, SelectionKey.OP_ACCEPT);
  }

  public void registerWrite(SelectableChannel channel, SelectHandler callback)
      throws ClosedChannelException {
    addInterest(channel, SelectionKey.OP_WRITE, callback);
  }

  public void unregisterWrite(SelectableChannel channel) {
    removeInterest(channel, SelectionKey.OP_WRITE);
  }

  public boolean isWriteRegistered(SelectableChannel channel) {
    return isInterestRegistered(channel, SelectionKey.OP_WRITE);
  }

  private void addInterest(SelectableChannel channel,
                           int operation,
                           SelectHandler callback)
      throws ClosedChannelException {

    SelectionKey key = channel.keyFor(selector);

    if (key == null) {
      channel.register(selector, operation, callback);
    } else if (!key.isValid()) {
      throw new RuntimeException(
          String.format("Unable to add %d in %s due to key is invalid", operation, channel));
    } else {
      // Key is not null and key is valid
      if ((key.interestOps() & operation) != 0) {
        LOG.severe(String.format("%d has been registered in %s", operation, channel));
//        throw new RuntimeException(
//            String.format("%d has been registered in %s", operation, channel));
      }
      if (key.attachment() == null) {
        key.attach(callback);
      } else {
        if (callback != key.attachment()) {
          throw new RuntimeException("Unmatched SelectHandler has already been attached"
              + " for other operation");
        }
      }
      key.interestOps(key.interestOps() | operation);
    }
  }

  private void removeInterest(SelectableChannel channel, int operation) {
    SelectionKey key = channel.keyFor(selector);

    // Exception would be thrown if key is null or key is inValid
    // We do not need double check it ahead
    key.interestOps(key.interestOps() & (~operation));
  }

  private boolean isInterestRegistered(SelectableChannel channel, int operation) {
    SelectionKey key = channel.keyFor(selector);

    return key != null && (key.interestOps() & operation) != 0;
  }

  public void removeAllInterest(SelectableChannel channel) {
    SelectionKey key = channel.keyFor(selector);
    if (key != null) {
      key.cancel();
    }
  }
}
