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
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.dfw.MessageDirection;
import edu.iu.dsc.tws.comms.dfw.io.types.PartialDataDeserializer;
import edu.iu.dsc.tws.comms.dfw.io.types.PartialKeyDeSerializer;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public class UnifiedDeserializer implements MessageDeSerializer {
  private static final Logger LOG = Logger.getLogger(UnifiedDeserializer.class.getName());

  private KryoSerializer serializer;

  private int executor;

  private boolean keyed;

  public  UnifiedDeserializer(KryoSerializer kryoSerializer, int exec) {
    this.serializer = kryoSerializer;
    this.executor = exec;
  }

  @Override
  public void init(Config cfg, boolean k) {
    this.keyed = k;
  }

  private int bufferCount = 0;

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
      bufferCount++;

      // if we are at the begining
      int currentObjectLength = currentMessage.getUnPkCurrentObjectLength();
      if (currentMessage.getUnPkBuffers() == 0) {
        currentLocation = 16;
      }

      if (currentObjectLength == -1 || currentMessage.getUnPkBuffers() == 0) {
        currentObjectLength = buffer.getByteBuffer().getInt(currentLocation);
        LOG.info(String.format("%d number %d header read 1, buffer count %d lenght %d",
            executor, header.getNumberTuples(), bufferCount, currentObjectLength));
        remaining = buffer.getSize() - Integer.BYTES - 16;
        currentLocation += Integer.BYTES;

        if (keyed) {
          // we assume we can read the key length from here
          Pair<Integer, Integer> keyLength = PartialKeyDeSerializer.createKey(
              currentMessage, buffer);
          // we advance the key length amount
          currentLocation += keyLength.getRight();
          PartialDataDeserializer.createDataObject(currentMessage, currentObjectLength);
          currentMessage.setUnPkCurrentKeyLength(keyLength.getLeft());
          currentMessage.setUnPkCurrentObjectLength(currentObjectLength);
        } else {
          PartialDataDeserializer.createDataObject(currentMessage, currentObjectLength);
          currentMessage.setUnPkCurrentObjectLength(currentObjectLength);
        }
      }

      while (remaining > 0) {
        // read the values from the buffer
        int valsRead = PartialDataDeserializer.readFromBuffer(currentMessage, currentLocation,
            buffer, currentObjectLength, serializer);
        int totalBytesRead = PartialDataDeserializer.totalBytesRead(currentMessage, valsRead);
        currentLocation += valsRead;
        // okay we are done with this object
        if (totalBytesRead == currentObjectLength) {
          // lets add the object
          remaining = remaining - valsRead;
          currentMessage.addCurrentObject();
          // lets reset to read the next
          currentMessage.resetUnPk();
        } else {
          // lets break the inner while loop
          break;
        }

        if (remaining >= Integer.BYTES) {
          currentObjectLength = buffer.getByteBuffer().getInt(currentLocation);
          remaining = remaining - Integer.BYTES;
          currentLocation += Integer.BYTES;

          PartialDataDeserializer.createDataObject(currentMessage, currentObjectLength);
          currentMessage.setUnPkCurrentObjectLength(currentObjectLength);
        } else {
          // we have to break here as we cannot read further
          break;
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
//        LOG.info(String.format("%d read objects %d buffers %d", executor,
//            readObjectNumber, builtBuffers.size()));
        break;
      }

      // lets move to next
      buffer = buffers.peek();
    }


    if (builtBuffers.size() > 0) {
      ChannelMessage channelMessage = new ChannelMessage(currentMessage.getOriginatingId(),
          currentMessage.getDataType(), MessageDirection.IN, currentMessage.getReleaseListener());
      channelMessage.addBuffers(builtBuffers);
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
