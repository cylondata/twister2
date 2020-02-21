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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.messaging.ChannelMessage;
import edu.iu.dsc.tws.api.comms.messaging.MessageDirection;
import edu.iu.dsc.tws.api.comms.messaging.MessageHeader;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.api.comms.packing.DataPacker;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.comms.dfw.InMessage;

public class FixedSchemaKeyedDataDeSerializer extends KeyedDataDeSerializer {

  private static final Logger LOG = Logger.getLogger(
      FixedSchemaKeyedDataDeSerializer.class.getName()
  );

  private MessageSchema messageSchema;

  public FixedSchemaKeyedDataDeSerializer(MessageSchema messageSchema) {
    this.messageSchema = messageSchema;
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
        currentObjectLength = messageSchema.getMessageSize();
      }

      if (currentKeyLength == -1) {
        // we have to set the current object length
        currentObjectLength = messageSchema.getMessageSize() - messageSchema.getKeySize();
        currentKeyLength = messageSchema.getKeySize();

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

          if (remaining > 0) {
            currentObjectLength = messageSchema.getMessageSize();

            // we have to set the current object length
            currentObjectLength = currentObjectLength - messageSchema.getKeySize();
            currentKeyLength = messageSchema.getKeySize();

            currentMessage.getKeyBuilder().init(keyPacker, currentKeyLength);

            currentMessage.getDataBuilder().init(dataPacker, currentObjectLength);

            currentMessage.setUnPkCurrentKeyLength(currentKeyLength);
            currentMessage.setUnPkCurrentObjectLength(currentObjectLength);
            // we are going to read the key first
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
}
