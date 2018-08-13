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
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

/**
 * Base network channel
 */
public abstract class BaseNetworkChannel {
  private static final Logger LOG = Logger.getLogger(BaseNetworkChannel.class.getName());

  protected BlockingQueue<TCPMessage> pendingSends;

  protected Map<Integer, BlockingQueue<TCPMessage>> pendingReceives;

  protected final SocketChannel socketChannel;

  protected Progress looper;

  protected SelectHandler selectHandler;

  protected ByteBuffer readHeader;

  protected ByteBuffer writeHeader;

  protected TCPMessage readingRequest;

  protected int readEdge;

  protected int readMessageSize;

  protected DataStatus readStatus;

  protected DataStatus writeStatus;

  protected ChannelHandler channelHandler;

  // header size of each message, we use edge and length as the header
  private static final int HEADER_SIZE = 8;

  BaseNetworkChannel(Config cfg, Progress progress, SelectHandler handler,
                     SocketChannel channel, ChannelHandler msgHandler) {
    this.socketChannel = channel;
    this.selectHandler = handler;
    this.looper = progress;
    this.channelHandler = msgHandler;

    pendingSends = new ArrayBlockingQueue<>(1024);
    pendingReceives = new HashMap<>();

    readHeader = ByteBuffer.allocate(HEADER_SIZE);
    writeHeader = ByteBuffer.allocate(HEADER_SIZE);

    this.readStatus = DataStatus.INIT;
    this.writeStatus = DataStatus.INIT;
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

  public abstract TCPMessage readRequest(SocketChannel channel);

  public void clear() {
    pendingReceives.clear();
    pendingSends.clear();
  }

  public boolean addReadRequest(TCPMessage request) {
    return false;
  }

  public boolean addWriteRequest(TCPMessage request) {
    ByteBuffer byteBuffer = request.getByteBuffer();
    if (request.getLength() == 0) {
      throw new RuntimeException("Cannot send a message with 0 length");
    }

    if (byteBuffer.remaining() == 0) {
      throw new RuntimeException("Cannot send a message with 0 length");
    }

    return pendingSends.offer(request);
  }

  public void write() {
    while (pendingSends.size() > 0) {
      TCPMessage writeRequest = pendingSends.peek();
      if (writeRequest == null) {
        break;
      }

      int writeState = writeRequest(socketChannel, writeRequest);
      if (writeState > 0) {
        break;
      } else if (writeState < 0) {
        LOG.severe("Something bad happened while writing to channel");
        selectHandler.handleError(socketChannel);
        return;
      } else {
        LOG.finest("Send complete");
        // remove the request
        pendingSends.poll();
        writeRequest.setComplete(true);
        // notify the handler
        channelHandler.onSendComplete(socketChannel, writeRequest);
        if (pendingSends.size() == 0) {
          disableWriting();
        }
      }
    }

  }

  private int writeRequest(SocketChannel channel, TCPMessage message) {
    ByteBuffer buffer = message.getByteBuffer();
    int written = 0;
    if (writeStatus == DataStatus.INIT) {
      // lets flip the buffer
      writeHeader.clear();
      writeStatus = DataStatus.HEADER;

      writeHeader.putInt(message.getLength());
      writeHeader.putInt(message.getEdge());
      writeHeader.flip();
      LOG.finest(String.format("WRITE Header %d %d", message.getLength(), message.getEdge()));
    }

    if (writeStatus == DataStatus.HEADER) {
      written = writeToChannel(channel, writeHeader);
      if (written < 0) {
        return written;
      } else if (written == 0) {
        writeStatus = DataStatus.BODY;
      }
    }

    if (writeStatus == DataStatus.BODY) {
      written = writeToChannel(channel, buffer);
      if (written < 0) {
        return written;
      } else if (written == 0) {
        LOG.finest(String.format("WRITE BODY %d", buffer.limit()));
        writeStatus = DataStatus.INIT;
        return written;
      }
    }

    return written;
  }

  private int writeToChannel(SocketChannel channel, ByteBuffer buffer) {
    int remaining = buffer.remaining();
    assert remaining > 0;
    int wrote;
    try {
      wrote = channel.write(buffer);
      LOG.finest("Wrote " + wrote);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error writing to channel ", e);
      return -1;
    }
    return remaining - wrote;
  }

  int readFromChannel(SocketChannel channel, ByteBuffer buffer) {
    int remaining = buffer.remaining();
    int read;
    try {
      read = channel.read(buffer);
      LOG.finest("Read size: " + read);
    } catch (java.nio.channels.ClosedByInterruptException e) {
      LOG.warning("ClosedByInterruptException thrown. "
          + "Probably the Channel is closed by the user program intentionally.");
      return -1;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error in channel.read ", e);
      return -1;
    }
    if (read < 0) {
      LOG.log(Level.SEVERE, "channel read returned negative " + read);
      return read;
    } else {
      return remaining - read;
    }
  }

  public void forceFlush() {
    while (!pendingSends.isEmpty()) {
      int writeState = writeRequest(socketChannel, pendingSends.poll());
      if (writeState != 0) {
        return;
      }
    }
  }

  public boolean isPending() {
    boolean sendPending = pendingSends.size() > 0;
    boolean recvPending = false;
    for (Map.Entry<Integer, BlockingQueue<TCPMessage>> e : pendingReceives.entrySet()) {
      recvPending = e.getValue().size() > 0;
    }
    return sendPending || recvPending;
  }

  public void enableReading() {
    if (!looper.isReadRegistered(socketChannel)) {
      try {
        looper.registerRead(socketChannel, selectHandler);
      } catch (ClosedChannelException e) {
        selectHandler.handleError(socketChannel);
      }
    }
  }

  public void disableReading() {
    if (looper.isReadRegistered(socketChannel)) {
      looper.unregisterRead(socketChannel);
    }
  }

  public void enableWriting() {
    if (!looper.isWriteRegistered(socketChannel)) {
      try {
        looper.registerWrite(socketChannel, selectHandler);
      } catch (ClosedChannelException e) {
        selectHandler.handleError(socketChannel);
      }
    }
  }

  public void disableWriting() {
    if (looper.isWriteRegistered(socketChannel)) {
      looper.unregisterWrite(socketChannel);
    }
  }
}
