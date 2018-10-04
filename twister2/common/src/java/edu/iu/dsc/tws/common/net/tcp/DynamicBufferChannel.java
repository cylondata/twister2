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
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

public class DynamicBufferChannel extends BaseNetworkChannel {
  private static final Logger LOG = Logger.getLogger(DynamicBufferChannel.class.getName());

  public DynamicBufferChannel(Config cfg, Progress progress, SelectHandler handler,
                            SocketChannel channel, ChannelHandler msgHandler) {
    super(cfg, progress, handler, channel, msgHandler);
  }

  public void read() {
    TCPMessage readRequest = readRequest(socketChannel);
    while (readRequest != null) {
      readRequest.setComplete(true);
      channelHandler.onReceiveComplete(socketChannel, readRequest);
      readRequest = readRequest(socketChannel);
    }
  }

  @Override
  public boolean addReadRequest(TCPMessage request) {
    throw new UnsupportedOperationException("In dynamic mode, read requests are not added");
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
        readingRequest = new TCPMessage(ByteBuffer.allocate(readMessageSize),
            readEdge, readMessageSize);
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
        LOG.severe("Failed to read");

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
      }
    }
    return null;
  }
}
