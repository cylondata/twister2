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

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

public class FixedBufferChannel extends BaseNetworkChannel {
  private static final Logger LOG = Logger.getLogger(FixedBufferChannel.class.getName());

  public FixedBufferChannel(Config cfg, Progress progress, SelectHandler handler,
                            SocketChannel channel, ChannelHandler msgHandler) {
    super(cfg, progress, handler, channel, msgHandler);
  }

  public void read() {
    while (pendingReceives.size() > 0) {
      TCPMessage readRequest = readRequest(socketChannel);

      if (readRequest != null) {
        readRequest.setComplete(true);
        channelHandler.onReceiveComplete(socketChannel, readRequest);
      } else {
        break;
      }
    }
  }

  @Override
  public boolean addReadRequest(TCPMessage request) {
    BlockingQueue<TCPMessage> inQueue = null;
    if (pendingReceives.containsKey(request.getEdge())) {
      inQueue = pendingReceives.get(request.getEdge());
    } else {
      inQueue = new ArrayBlockingQueue<>(1024);
      pendingReceives.put(request.getEdge(), inQueue);
    }
    return inQueue.offer(request);
  }

  /**
   * Read the request
   * @param channel
   * @return
   */
  public TCPMessage readRequest(SocketChannel channel) {
    if (readStatus == DataStatus.INIT) {
      readHeader.clear();
      readStatus = DataStatus.HEADER;
      LOG.finest("READ Header INIT");
    }

    if (readStatus == DataStatus.HEADER) {
      int retval = readFromChannel(channel, readHeader);
      if (retval != 0) {
        // either we didnt read fully or we had an error
        // we had an error
        if (retval < 0) {
          selectHandler.handleError(channel);
        }
        return null;
      }

      // We read the header fully
      readHeader.flip();
      readMessageSize = readHeader.getInt();
      readEdge = readHeader.getInt();
      readStatus = DataStatus.BODY;
      LOG.finest(String.format("READ Header %d %d", readMessageSize, readEdge));
    }

    if (readStatus == DataStatus.BODY) {
      ByteBuffer buffer;
      if (readingRequest == null) {
        Queue<TCPMessage> readRequests = getReadRequest(readEdge);
        if (readRequests.size() == 0) {
          return null;
        }
        readingRequest = readRequests.poll();
        buffer = readingRequest.getByteBuffer();
        buffer.limit(readMessageSize);
      } else {
        buffer = readingRequest.getByteBuffer();
      }

      int retVal = readFromChannel(channel, buffer);
      if (retVal < 0) {
        readMessageSize = 0;
        readEdge = 0;

        readingRequest = null;
        readStatus = DataStatus.INIT;
        LOG.log(Level.SEVERE, "Failed to read");
        // we had an error
        selectHandler.handleError(channel);
        // handle the error
        return null;
      } else if (retVal == 0) {
        readMessageSize = 0;
        readEdge = 0;
        buffer.flip();

        TCPMessage ret = readingRequest;
        readingRequest = null;
        readStatus = DataStatus.INIT;
        LOG.finest(String.format("READ Body %d", buffer.limit()));
        return ret;
      } else {
        return null;
      }
    }
    return null;
  }

  private BlockingQueue<TCPMessage> getReadRequest(int e) {
    BlockingQueue<TCPMessage> readRequests = pendingReceives.get(e);
    if (readRequests == null) {
      readRequests = new ArrayBlockingQueue<>(1024);
      pendingReceives.put(e, readRequests);
    }
    return readRequests;
  }
}
