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
package edu.iu.dsc.tws.comms.tcp.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

public class Channel {
  private static final Logger LOG = Logger.getLogger(Channel.class.getName());

  private Queue<TCPRequest> pendingSends;

  private Map<Integer, Queue<TCPRequest>> pendingReceives;

  private final SocketChannel socketChannel;

  private Progress looper;

  private SelectHandler selectHandler;

  private ByteBuffer readHeader;

  private ByteBuffer writeHeader;

  private TCPRequest readingRequest;

  private int readEdge;

  private int readSize;

  private enum DataStatus {
    INIT,
    HEADER,
    BODY
  }

  private DataStatus readStatus;

  private DataStatus writeStatus;

  private MessageHandler messageHandler;

  // header size of each message, we use edge and length as the header
  private static final int HEADER_SIZE = 8;

  public Channel(Config cfg, Progress progress, SelectHandler handler,
                 SocketChannel channel, MessageHandler msgHandler) {
    this.socketChannel = channel;
    this.selectHandler = handler;
    this.looper = progress;
    this.messageHandler = msgHandler;

    pendingSends = new ArrayBlockingQueue<>(1024);
    pendingReceives = new HashMap<>();

    readHeader = ByteBuffer.allocate(HEADER_SIZE);
    writeHeader = ByteBuffer.allocate(HEADER_SIZE);

    this.readStatus = DataStatus.INIT;
    this.writeStatus = DataStatus.INIT;
  }

  public void read() {
//    LOG.info("Reading from channel: " + socketChannel);
    while (pendingReceives.size() > 0) {
      TCPRequest readRequest = readRequest(socketChannel);

      if (readRequest != null) {
        readRequest.setComplete(true);
        messageHandler.onReceiveComplete(socketChannel, readRequest);
      } else {
        break;
      }
    }
  }

  public void clear() {
    pendingReceives.clear();
    pendingSends.clear();
  }

  public boolean addReadRequest(TCPRequest request) {
    Queue<TCPRequest> readRequests = getReadRequest(request.getEdge());
    ByteBuffer byteBuffer = request.getByteBuffer();
    byteBuffer.position(0);
    byteBuffer.limit(request.getLength());

    return readRequests.offer(request);
  }

  public boolean addWriteRequest(TCPRequest request) {
    ByteBuffer byteBuffer = request.getByteBuffer();
    byteBuffer.position(request.getLength());

    return pendingSends.offer(request);
  }

  public void write() {
    while (pendingSends.size() > 0) {
      TCPRequest writeRequest = pendingSends.peek();
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
        LOG.log(Level.INFO, "Send complete");
        // remove the request
        pendingSends.poll();
        writeRequest.setComplete(true);
        // notify the handler
        messageHandler.onSendComplete(socketChannel, writeRequest);
      }
    }

//    if (pendingSends.size() == 0) {
//      disableWriting();
//    }
  }

  private int writeRequest(SocketChannel channel, TCPRequest request) {
    ByteBuffer buffer = request.getByteBuffer();
    int written = 0;
    if (writeStatus == DataStatus.INIT) {
      // lets flip the buffer
      writeHeader.clear();
      writeStatus = DataStatus.HEADER;

      writeHeader.putInt(request.getLength());
      writeHeader.putInt(request.getEdge());
      writeHeader.flip();
      LOG.log(Level.INFO, String.format("WRITE Header %d %d",
          request.getLength(), request.getEdge()));
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
      buffer.flip();
      written = writeToChannel(channel, buffer);
      if (written < 0) {
        return written;
      } else if (written == 0) {
        LOG.log(Level.INFO, String.format("WRITE BODY %d", buffer.limit()));
        writeStatus = DataStatus.INIT;
        return written;
      }
    }

    return written;
  }

  private int writeToChannel(SocketChannel channel, ByteBuffer buffer) {
    int remaining = buffer.remaining();
    assert remaining > 0;
    int wrote = 0;
    try {
      wrote = channel.write(buffer);
      LOG.info("Wrote " + wrote);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error writing to channel ", e);
      return -1;
    }
    return remaining - wrote;
  }

  private TCPRequest readRequest(SocketChannel channel) {
    if (readStatus == DataStatus.INIT) {
      readHeader.clear();
      readStatus = DataStatus.HEADER;
      LOG.log(Level.INFO, "READ Header INIT");
    }

    if (readStatus == DataStatus.HEADER) {
      int retval = readFromChannel(channel, readHeader);
      if (retval != 0) {
        // either we didnt read fully or we had an error
        return null;
      }

      // We read the header fully
      readHeader.flip();
      readSize = readHeader.getInt();
      readEdge = readHeader.getInt();
      readStatus = DataStatus.BODY;
      LOG.log(Level.INFO, String.format("READ Header %d %d", readSize, readEdge));
    }

    if (readStatus == DataStatus.BODY) {
      ByteBuffer buffer;
      if (readingRequest == null) {
        Queue<TCPRequest> readRequests = getReadRequest(readEdge);
        if (readRequests.size() == 0) {
          return null;
        }
        readingRequest = readRequests.poll();
        buffer = readingRequest.getByteBuffer();
        buffer.limit(readSize);
      } else {
        buffer = readingRequest.getByteBuffer();
      }

      int retVal = readFromChannel(channel, buffer);
      if (retVal < 0) {
        readSize = 0;
        readEdge = 0;

        readingRequest = null;
        readStatus = DataStatus.INIT;
        LOG.log(Level.SEVERE, "Failed to read");
        // handle the error
        return null;
      } else if (retVal == 0) {
        readSize = 0;
        readEdge = 0;
        buffer.flip();

        TCPRequest ret = readingRequest;
        readingRequest = null;
        readStatus = DataStatus.INIT;
        LOG.log(Level.INFO, String.format("READ Body %d", buffer.limit()));
        return ret;
      } else {
        LOG.log(Level.INFO, String.format("READ Body not COMPLETE %d %d", buffer.limit(), retVal));
        return null;
      }
    }
    return null;
  }

  private int readFromChannel(SocketChannel channel, ByteBuffer buffer) {
    int remaining = buffer.remaining();
    int read;
    try {
      read = channel.read(buffer);
      LOG.log(Level.INFO, "Read size: " + read);
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

  private Queue<TCPRequest> getReadRequest(int e) {
    Queue<TCPRequest> readRequests = pendingReceives.get(e);
    if (readRequests == null) {
      readRequests = new ArrayBlockingQueue<>(1024);
      pendingReceives.put(e, readRequests);
    }
    return readRequests;
  }
}
