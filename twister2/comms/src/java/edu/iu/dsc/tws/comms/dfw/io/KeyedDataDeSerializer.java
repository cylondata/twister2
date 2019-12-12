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
package edu.iu.dsc.tws.comms.dfw.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import edu.iu.dsc.tws.api.comms.messaging.ChannelMessage;
import edu.iu.dsc.tws.api.comms.messaging.MessageDirection;
import edu.iu.dsc.tws.api.comms.messaging.MessageFlags;
import edu.iu.dsc.tws.api.comms.messaging.MessageHeader;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.api.comms.packing.DataPacker;
import edu.iu.dsc.tws.api.comms.packing.MessageDeSerializer;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.dfw.InMessage;

public class KeyedDataDeSerializer implements MessageDeSerializer {

  @Override
  public void init(Config cfg) {
  }

  /**
   * Builds the message from the data buffers in the partialObject. Since this method
   * supports multi-messages it iterates through the buffers and builds all the messages separately
   *
   * @param partialObject message object that needs to be built
   * @param edge the edge value associated with this message
   */
  @Override
  public void build(Object partialObject, int edge) {
    InMessage currentMessage = (InMessage) partialObject;
    MessageType keyType = currentMessage.getKeyType();
    DataPacker keyPacker = keyType.getDataPacker();
    DataPacker dataPacker = currentMessage.getDataType().getDataPacker();
    Queue<DataBuffer> buffers = currentMessage.getBuffers();
    MessageHeader header = currentMessage.getHeader();

    if (header == null) {
      throw new RuntimeException("Header must be built before the message");
    }

    if ((header.getFlags() & MessageFlags.SYNC_BARRIER) == MessageFlags.SYNC_BARRIER) {
      keyType = MessageTypes.BYTE_ARRAY;
      keyPacker = MessageTypes.BYTE_ARRAY.getDataPacker();

      dataPacker = keyPacker;
    }

    List<DataBuffer> builtBuffers = new ArrayList<>();
    // get the number of objects deserialized
    DataBuffer buffer = buffers.peek();
    while (buffer != null) {
      int currentLocation = 0;
      int remaining = buffer.getSize();

      if (header.getNumberTuples() == 0) {
        builtBuffers.add(buffer);
        break;
      }

      // if we are at the beginning
      int currentObjectLength = currentMessage.getUnPkCurrentObjectLength();
      int currentKeyLength = currentMessage.getUnPkCurrentKeyLength();

      if (currentMessage.getUnPkBuffers() == 0) {
        currentLocation = DFWIOUtils.HEADER_SIZE;
        remaining = remaining - DFWIOUtils.HEADER_SIZE;
      } else {
        currentLocation = DFWIOUtils.SHORT_HEADER_SIZE;
        remaining = remaining - DFWIOUtils.SHORT_HEADER_SIZE;
      }

      if (currentObjectLength == -1 || currentMessage.getUnPkBuffers() == 0) {
        currentObjectLength = buffer.getByteBuffer().getInt(currentLocation);
        remaining = remaining - Integer.BYTES;
        currentLocation += Integer.BYTES;
      }

      if (currentKeyLength == -1) {
        // we assume we can read the key length from here
        int right = DataPackerProxy.getKeyLengthRight(keyType, buffer, currentLocation);
        int left = DataPackerProxy.getKeyLengthLeft(keyType, buffer, currentLocation);

        remaining = remaining - right;
        currentLocation += right;

        // we have to set the current object length
        currentObjectLength = currentObjectLength - left - right;
        currentKeyLength = left;

        currentMessage.getKeyBuilder().init(keyPacker, currentKeyLength);

        try {
          currentMessage.getDataBuilder().init(dataPacker, currentObjectLength);


          currentMessage.setUnPkCurrentKeyLength(currentKeyLength);
          currentMessage.setUnPkCurrentObjectLength(currentObjectLength);
          // we are going to read the key first
          currentMessage.setReadingKey(true);
        } catch (NegativeArraySizeException e) {
          throw new RuntimeException(e);
        }
      }

      while (remaining > 0) {
        if (currentMessage.isReadingKey()) {
          ObjectBuilderImpl keyBuilder = currentMessage.getKeyBuilder();
          int bytesRead = keyPacker.readDataFromBuffer(
              keyBuilder,
              currentLocation,
              buffer
          );

          keyBuilder.incrementCompletedSizeBy(bytesRead);

          currentLocation += bytesRead;
          remaining = remaining - bytesRead;

          currentMessage.setReadingKey(!keyBuilder.isBuilt());

          if (keyBuilder.isBuilt()) {
            //done reading key
            currentMessage.setReadingKey(false);
          } else {
            break;
          }
        }

        if (!currentMessage.isReadingKey()) {
          ObjectBuilderImpl dataBuilder = currentMessage.getDataBuilder();
          // read the values from the buffer
          int byteRead = dataPacker.readDataFromBuffer(
              dataBuilder,
              currentLocation,
              buffer
          );
          dataBuilder.incrementCompletedSizeBy(byteRead);

          currentLocation += byteRead;
          remaining = remaining - byteRead;
          // okay we are done with this object
          if (dataBuilder.isBuilt()) {
            // lets add the object
            currentMessage.addCurrentKeyedObject();
          } else {
            // lets break the inner while loop
            break;
          }

          int bytesToReadKey = 0;
          if (keyPacker.isHeaderRequired()) {
            bytesToReadKey += Integer.BYTES;
          }

          if (remaining >= Integer.BYTES + bytesToReadKey) {
            currentObjectLength = buffer.getByteBuffer().getInt(currentLocation);
            remaining = remaining - Integer.BYTES;
            currentLocation += Integer.BYTES;

            // we assume we can read the key length from here
            int right = DataPackerProxy.getKeyLengthRight(keyType, buffer, currentLocation);
            int left = DataPackerProxy.getKeyLengthLeft(keyType, buffer, currentLocation);

            remaining = remaining - right;
            currentLocation += right;

            // we have to set the current object length
            currentObjectLength = currentObjectLength - left - right;
            currentKeyLength = left;

            currentMessage.getKeyBuilder().init(keyPacker, currentKeyLength);

            currentMessage.getDataBuilder().init(dataPacker, currentObjectLength);

            currentMessage.setUnPkCurrentKeyLength(currentKeyLength);
            currentMessage.setUnPkCurrentObjectLength(currentObjectLength);
            // we are going to read the key first
            currentMessage.setReadingKey(true);
          } else if (remaining >= Integer.BYTES) {
            currentObjectLength = buffer.getByteBuffer().getInt(currentLocation);
            remaining = remaining - Integer.BYTES;
            currentLocation += Integer.BYTES;

            currentMessage.setUnPkCurrentObjectLength(currentObjectLength);
            currentMessage.setUnPkCurrentKeyLength(-1);
            currentMessage.setReadingKey(true);
          } else {
            // we have to break here as we cannot read further
            break;
          }
        }
      }

      // lets remove this buffer
      buffers.poll();
      builtBuffers.add(buffer);
      // increment the unpacked buffers
      currentMessage.incrementUnPkBuffers();

      // lets check weather we have read everythong
      int readObjectNumber = currentMessage.getUnPkNumberObjects();
      // we need to get number of tuples and get abs because we are using -1 for single messages
      if (readObjectNumber == Math.abs(currentMessage.getHeader().getNumberTuples())) {
        break;
      }

      // lets move to next
      buffer = buffers.peek();
    }


    if (builtBuffers.size() > 0) {
      ChannelMessage channelMessage = new ChannelMessage(currentMessage.getOriginatingId(),
          currentMessage.getDataType(), MessageDirection.IN, currentMessage.getReleaseListener());
      channelMessage.addBuffers(builtBuffers);
      channelMessage.setHeader(currentMessage.getHeader());
      channelMessage.incrementRefCount();
      currentMessage.addBuiltMessage(channelMessage);
    }
  }

  @Override
  public Object getDataBuffers(Object partialObject, int edge) {
    return null;
  }

  /**
   * Builds the header object from the data in the data buffer
   *
   * @param buffer data buffer that contains the message
   * @param edge the edge value associated with this message
   * @return the built message header object
   */
  @Override
  public MessageHeader buildHeader(DataBuffer buffer, int edge) {
    int sourceId = buffer.getByteBuffer().getInt(Integer.BYTES);
    int flags = buffer.getByteBuffer().getInt(Integer.BYTES * 2);
    int destId = buffer.getByteBuffer().getInt(Integer.BYTES * 3);
    int length = buffer.getByteBuffer().getInt(Integer.BYTES * 4);

    MessageHeader.Builder headerBuilder = MessageHeader.newBuilder(
        sourceId, edge, length);
    headerBuilder.flags(flags);
    headerBuilder.destination(destId);
    return headerBuilder.build();
  }
}
