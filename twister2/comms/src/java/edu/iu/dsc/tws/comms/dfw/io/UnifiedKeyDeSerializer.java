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

import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataPacker;
import edu.iu.dsc.tws.comms.api.KeyPacker;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.dfw.MessageDirection;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public class UnifiedKeyDeSerializer implements MessageDeSerializer {
  private static final Logger LOG = Logger.getLogger(UnifiedKeyDeSerializer.class.getName());

  private DataPacker dataPacker;

  private KeyPacker keyPacker;

  public UnifiedKeyDeSerializer(KryoSerializer kryoSerializer, int exec,
                                MessageType keyType, MessageType dataType) {
    dataPacker = DFWIOUtils.createPacker(dataType);
    keyPacker = DFWIOUtils.createKeyPacker(keyType);

    LOG.fine("Initializing serializer on worker: " + exec);
  }

  @Override
  public void init(Config cfg, boolean k) {
  }

  /**
   * Builds the message from the data buffers in the partialObject. Since this method
   * supports multi-messages it iterates through the buffers and builds all the messages separately
   *
   * @param partialObject message object that needs to be built
   * @param edge the edge value associated with this message
   * @return the built message as a list of objects
   */
  @Override
  public Object build(Object partialObject, int edge) {
    InMessage currentMessage = (InMessage) partialObject;
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

      // if we are at the begining
      int currentObjectLength = currentMessage.getUnPkCurrentObjectLength();
      int currentKeyLength = currentMessage.getUnPkCurrentKeyLength();

      if (currentMessage.getUnPkBuffers() == 0) {
        currentLocation = 16;
      }

      if (currentObjectLength == -1 || currentMessage.getUnPkBuffers() == 0) {
        currentObjectLength = buffer.getByteBuffer().getInt(currentLocation);
        remaining = buffer.getSize() - Integer.BYTES - 16;
        currentLocation += Integer.BYTES;
      }

      if (currentKeyLength == -1) {
        // we assume we can read the key length from here
        Pair<Integer, Integer> keyLength = keyPacker.getKeyLength(
            currentMessage, buffer, currentLocation);
        remaining = remaining - keyLength.getRight();
        currentLocation += keyLength.getRight();

        // we have to set the current object length
        currentObjectLength = currentObjectLength - keyLength.getLeft();
        currentKeyLength = keyLength.getLeft();

        Object keyValue = keyPacker.initializeUnPackKeyObject(keyLength.getLeft());
        currentMessage.setDeserializingKey(keyValue);
        currentMessage.setUnPkCurrentBytes(0);

        Object value = dataPacker.initializeUnPackDataObject(currentObjectLength);
        currentMessage.setDeserializingObject(value);
        currentMessage.setUnPkCurrentBytes(0);

        currentMessage.setUnPkCurrentKeyLength(currentKeyLength);
        currentMessage.setUnPkCurrentObjectLength(currentObjectLength);
        // we are going to read the key first
        currentMessage.setReadingKey(true);
      }

      while (remaining > 0) {
        if (currentMessage.isReadingKey()) {
          int valsRead = keyPacker.readKeyFromBuffer(currentMessage, currentLocation,
              buffer, currentKeyLength);
          int totalBytesRead = currentMessage.addUnPkCurrentBytes(valsRead);
          currentLocation += valsRead;
          remaining = remaining - valsRead;

          if (totalBytesRead == currentKeyLength) {
            currentMessage.resetUnPkKey();
          } else {
            break;
          }
        }

        if (!currentMessage.isReadingKey()) {
          // read the values from the buffer
          int valsRead = dataPacker.readDataFromBuffer(currentMessage, currentLocation,
              buffer, currentObjectLength);
          int totalBytesRead = currentMessage.addUnPkCurrentBytes(valsRead);
          currentLocation += valsRead;
          remaining = remaining - valsRead;
          // okay we are done with this object
          if (totalBytesRead == currentObjectLength) {
            // lets add the object
            currentMessage.addCurrentKeyedObject();
            // lets reset to read the next
            currentMessage.resetUnPk();
          } else {
            // lets break the inner while loop
            break;
          }

          int bytesToReadKey = 0;
          if (keyPacker.isKeyHeaderRequired()) {
            bytesToReadKey += Integer.BYTES;
          }

          if (remaining >= Integer.BYTES + bytesToReadKey) {
            currentObjectLength = buffer.getByteBuffer().getInt(currentLocation);
            remaining = remaining - Integer.BYTES;
            currentLocation += Integer.BYTES;

            // we assume we can read the key length from here
            Pair<Integer, Integer> keyLength = keyPacker.getKeyLength(
                currentMessage, buffer, currentLocation);
            remaining = remaining - keyLength.getRight();
            currentLocation += keyLength.getRight();

            // we have to set the current object length
            currentObjectLength = currentObjectLength - keyLength.getLeft();
            currentKeyLength = keyLength.getLeft();

            Object keyValue = keyPacker.initializeUnPackKeyObject(keyLength.getLeft());
            currentMessage.setDeserializingKey(keyValue);
            currentMessage.setUnPkCurrentBytes(0);

            Object value = dataPacker.initializeUnPackDataObject(currentObjectLength);
            currentMessage.setDeserializingObject(value);
            currentMessage.setUnPkCurrentBytes(0);

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
    return null;
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
    int sourceId = buffer.getByteBuffer().getInt(0);
    int flags = buffer.getByteBuffer().getInt(Integer.BYTES);
    int destId = buffer.getByteBuffer().getInt(Integer.BYTES * 2);
    int length = buffer.getByteBuffer().getInt(Integer.BYTES * 3);

    MessageHeader.Builder headerBuilder = MessageHeader.newBuilder(
        sourceId, edge, length);
    headerBuilder.flags(flags);
    headerBuilder.destination(destId);
    return headerBuilder.build();
  }
}
